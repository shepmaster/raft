(ns raft.core
  (:import (java.util.concurrent LinkedBlockingQueue TimeUnit))
  (:require [clojure.set :as set]))

(defprotocol RPC
  (rpc-send [this peer-info name args])
  (rpc-broadcast [this peer-infos name args])
  (rpc-receive [this peer-info timeout timeout-units]))

(defrecord InMemoryRPC [sockets]
  RPC
  (rpc-send [this peer-info name args]
    (let [socket (get sockets (:id peer-info))]
      (.put socket [name args]))
    this)

  (rpc-broadcast [this peer-infos name args]
    (doseq [peer-info peer-infos]
      (rpc-send this peer-info name args))
    this)

  (rpc-receive [this peer-info timeout timeout-units]
    (let [socket (get sockets (:id peer-info))]
      (.poll socket timeout timeout-units))))

(defn create-in-memory-rpc
  ([]
     (create-in-memory-rpc [0 1 2]))
  ([peer-ids]
     (->InMemoryRPC (zipmap peer-ids (repeatedly #(LinkedBlockingQueue.))))))

(defrecord RPCSend [to name args])
(defrecord RPCReply [name args])
(defrecord RPCBroadcast [name args])
(defrecord HeartbeatStart [])

;; --

;; The paper assumes that lists start at index 1, not 0 as
;; Clojure expects.
(def first-log-entry-index 0)
(def no-entries-log-index (dec first-log-entry-index))
(def no-entries-log-index? (partial = no-entries-log-index))

;; Does the same thing apply to the term? Maybe not, as it's never
;; used for an index? Even so, could still make a def to clear it up
;; as no log entries are in term 0...
(def no-entries-log-term 0)

(defn peer-info-by-id [server id]
  (first (filter #(= (:id %) id) (:peers server))))

(defn self-peer-info [server]
  (peer-info-by-id server (:id server)))

(defn other-peer-infos
  [server]
  (filter #(not= (:id %) (:id server)) (:peers server)))

(defn vote-count [server]
  (count (filter identity (vals (:votes server))))) ;; CHECK: use true? instead

(defn majority-count [server]
  (-> (:peers server)
      (count)
      (/ 2)
      (Math/ceil)
      (int)))

(defn init-log-vars
  ([server]
     (init-log-vars server []))
  ([server log]
     (assoc server
       :commit-index no-entries-log-index
       :log log)))

(defn rand-int-in
  "Creates a random integer between the lower (inclusive) and upper
  (exclusive) bounds"
  [min max]
  (+ min (rand-int (- max min))))

(def election-timeout-ms-min 150)
(def election-timeout-ms-max 300)

(defn create-server
  "peers must be a sequence of connection information with the key
  :id. This server should be included in peers"
  [id peers rpc]
  (-> {:id id
       :state :follower
       :peers (set peers)
       :rpc rpc
       :term 0
       :last-applied 0}
      (init-log-vars)))

(defn vote-for
  "Votes for the given server id. If a vote has already been granted
  during this term, does nothing."
  [server peer-id]
  (if-not (:voted-for server)
    (assoc server :voted-for peer-id)
    server))

(defn add-vote [server from-id granted]
  (assoc-in server [:votes from-id] granted))

(defn become-candidate [server]
  (let [my-id (:id server)]
    (-> server
        (update-in [:term] inc)
        (assoc :state :candidate)
        (vote-for my-id)
        (add-vote my-id true))))

(defn create-rpc-args
  "Creates core arguments for all RPC calls. `server` should be the
  server sending the message."
  [server]
  {:sender (:id server)
   :term (:term server)})

(defn voted-for? [server peer-id]
  (= (:voted-for server) peer-id))

(defn create-request-vote-response [server peer-id]
  (-> (create-rpc-args server)
      (assoc :vote-granted (voted-for? server peer-id))))

(defn follower? [server]
  (= (:state server) :follower))

(defn candidate? [server]
  (= (:state server) :candidate))

(defn leader? [server]
  (= (:state server) :leader))

(defn peer-ids [server]
  (map :id (:peers server)))

(defn peer-map
  "Creates a map from each peer-id to a constant value."
  [server v]
  (zipmap (peer-ids server) (repeat v)))

(defn init-leader [server]
  (assoc server
    :state :leader
    :next-index (peer-map server first-log-entry-index)
    :last-agreed-index (peer-map server no-entries-log-index)))

(defn become-leader? [server]
  (and (candidate? server)
       (>= (vote-count server) (majority-count server))))

(defn become-leader
  "Transitions to leader if the candidate has enough votes, otherwise
  does nothing."
  [server]
  (if (become-leader? server)
    (init-leader server)
    server))

(defn change-state-from-request-vote-response [server peer-id vote-granted]
  (-> server
      (add-vote peer-id vote-granted)
      (become-leader)))

(defn become-follower
  "If the passed-in term is newer than the current term, revert to
  follower state"
  [server term]
  (if (> term (:term server))
    (-> server
        (assoc :term term
               :state :follower)
        (dissoc :voted-for))
    server))

(defn log-length [server]
  (count (:log server)))

;; Message handlers

(defn handle-timeout [server _]
  (if (leader? server)
    {:server server}
    (let [server (become-candidate server)]
      {:server server
       :side-effects [(->RPCBroadcast :request-vote
                                      (create-rpc-args server))]})))

(defn handle-request-vote [server args]
  (let [sender (:sender args)
        server (vote-for server sender)]
    {:server server
     :side-effects [(->RPCReply :request-vote-response
                                (create-request-vote-response server sender))]}))

(defn handle-request-vote-response [server args]
  (let [{:keys [sender vote-granted]} args]
    {:server (change-state-from-request-vote-response server sender vote-granted)}))

(defn log-at-index [server idx]
  (nth (:log server) idx nil))

(defn log-at-index-is-term? [server idx term]
  (if-let [entry (log-at-index server idx)]
    (= term (:term entry))))

(defn log-at-index-is-not-term? [server idx term]
  (if-let [entry (log-at-index server idx)]
    (not= term (:term entry))))

(defn accept-append-entries? [server args]
  (let [{:keys [term previous-log-index previous-log-term]} args]
    (and
     (= (:term server) term)
     (or
      (no-entries-log-index? previous-log-index)
      (log-at-index-is-term? server previous-log-index previous-log-term)))))

(defn heartbeat? [args]
  (empty? (:entries args)))

(defn already-processed? [server entry idx]
  (log-at-index-is-term? server idx (:term entry)))

(defn conflict? [server entry idx]
  (log-at-index-is-not-term? server idx (:term entry)))

(defn discriminate-append-entries-request
  [server args & {:keys [commit-fn conflict-fn no-conflict-fn rejected-fn]}]
  (if (accept-append-entries? server args)
    (let [idx (inc (:previous-log-index args))
          entries (take 1 (:entries args))
          entry (first entries)]
      (cond
       (or (heartbeat? args)
           (already-processed? server entry idx))
       (commit-fn server (:commit-index args))

       (conflict? server entry idx)
       (conflict-fn server)

       (= (log-length server) idx)
       (no-conflict-fn server entries)

       :else
       (throw (Exception. "Unknown case"))))
    (rejected-fn server)))

(defn update-commit-index [server commit-index]
  (assoc-in server [:commit-index] commit-index))

(defn remove-last-log-entry [server]
  (update-in server [:log] butlast))

(defn add-log-entries [server entries]
  (update-in server [:log] concat entries))

(defn create-append-request-reply [server last-agreed-index]
  (->RPCReply :append-entries-response
              (-> (create-rpc-args server)
                  (assoc :last-agreed-index last-agreed-index))))

(defn create-accept-append-request [server args]
  (create-append-request-reply server
                               (+ (:previous-log-index args)
                                  (count (:entries args)))))

(defn create-reject-append-request [server]
  (create-append-request-reply server no-entries-log-index))

(defn handle-append-entries [server args]
  (discriminate-append-entries-request
   server args

   :commit-fn
   (fn commit [server commit-index]
     {:server  (update-commit-index server commit-index)
      :side-effects [(create-accept-append-request server args)]})

   :conflict-fn
   (fn conflict [server]
     {:server  (remove-last-log-entry server)})

   :no-conflict-fn
   (fn no-conflict [server entries]
     {:server  (add-log-entries server entries)})

   :rejected-fn
   (fn rejected [server]
     {:server  server
      :side-effects [(create-reject-append-request server)]})))

(defn create-append-entries-request [server peer-id]
  (let [next-index (get-in server [:next-index peer-id] first-log-entry-index)
        previous-index (dec next-index)
        entry-index (min (log-length server) next-index)]
    (-> (create-rpc-args server)
        (assoc :previous-log-index previous-index
               :previous-log-term (if (no-entries-log-index? previous-index)
                                    no-entries-log-term
                                    (:term (log-at-index server previous-index)))
               :entries (take 1 (drop entry-index (:log server)))))))

(defn quorum-last-agreed-index
  "Returns the smallest log index that a quorum of the servers
  (including this one, the leader) agree on"
  [server]
  (let [index-vals (-> (:last-agreed-index server)
                       (assoc (:id server) (log-length server))
                       (vals))
        quorum-size (majority-count server)]
    (first (take-last quorum-size (sort index-vals)))))

(defn new-commit-index [server]
  (let [last-agreed-index (quorum-last-agreed-index server)]
    (if (log-at-index-is-term? server last-agreed-index (:term server))
      last-agreed-index
      (:commit-index server))))

;; TODO: prevent dec beyond earliest
(defn handle-append-entries-response [server args]
  (let [peer-id (:sender args)
        last-agreed-index (:last-agreed-index args)]
    {:server
     (if (> last-agreed-index no-entries-log-index)
       (-> server
           (assoc-in [:next-index peer-id] (inc last-agreed-index))
           (assoc-in [:last-agreed-index peer-id] last-agreed-index)
           (#(assoc % :commit-index (new-commit-index %))))
       (update-in server [:next-index peer-id] dec))
     }))

(defn add-new-log-entry [server command committed-fn]
  (add-log-entries server [{:term (:term server),
                            :command command,
                            :committed-fn committed-fn}]))

(defn create-all-append-entries-requests [server]
  (map (fn [id] (->RPCSend id :append-entries (create-append-entries-request server id)))
       (map :id (other-peer-infos server))))

;; TODO: assert only leader
(defn handle-user-command [server args]
  (let [{:keys [command committed-fn]} args
        server (add-new-log-entry server command committed-fn)]
    {:server server
     :side-effects (create-all-append-entries-requests server)}))
  ;; inform user when done. Adding the function here means it gets sent too

(defn handle-heartbeat [server _]
  {:server server
   :side-effects (if (leader? server)
                   (create-all-append-entries-requests server))})

(def handlers
  {:timeout                 handle-timeout
   :request-vote            handle-request-vote
   :request-vote-response   handle-request-vote-response
   :append-entries          handle-append-entries
   :append-entries-response handle-append-entries-response
   :user-command            handle-user-command
   :heartbeat               handle-heartbeat})

;; Message routing

(defn receive-pure [server name args]
  (if-let [handler (name handlers)]
    (-> server
        (become-follower (:term args no-entries-log-term))
        (handler args))
    (throw (Exception. (str "Unknown message " name)))))

(defn receive* [server name args]
  (let [{:keys [server side-effects]} (receive-pure server name args)]
    (doseq [side-effect side-effects]
      (let [{:keys [sender]} args
            {:keys [rpc]} server
            {:keys [name args]} side-effect]
        (condp = (class side-effect)
          RPCBroadcast (send-off rpc rpc-broadcast (other-peer-infos server) name args)
          RPCReply (send-off rpc rpc-send (peer-info-by-id server sender) name args)
          RPCSend (send-off rpc rpc-send (peer-info-by-id server (:to side-effect)) name args))))
    server))

;; User commands

;; Testing has been: spin up "servers", step through transitions,
;; verify the queue details and states.

;; Integration test ideas -

;; Start 3 servers, expect that term will stabilize, one will be
;; leader, others follower

;; TODO: Should `receive` be pulled out / merged with the message
;; pump?  Probably. Need to figure out how / if RPC should be
;; different. Maybe having create make an agent is wrong?

;; TODO: unify the create-* RPC functions w.r.t. how they create records

;; TODO: check all state precondition things

;; TODO: discard out-of date messages

(defn message-pump [rpc server-agt timeout-ms switch]
  (when @switch
    (if-let [[name args] (rpc-receive rpc
                                      (self-peer-info @server-agt)
                                      timeout-ms
                                      TimeUnit/MILLISECONDS)]
      (send-off server-agt receive* name args)
      (send-off server-agt receive* :timeout nil))
    (recur rpc server-agt timeout-ms switch)))

(defn create-message-in [rpc server-agt]
  (let [switch (atom true)
        timeout-ms (rand-int-in election-timeout-ms-min election-timeout-ms-max)]
    {:switch switch
     :timeout-ms timeout-ms
     :pump (Thread. (partial message-pump rpc server-agt timeout-ms switch))}))

(defn heartbeat [server-agt]
  (send-off server-agt receive* :heartbeat nil))

(defn create-heartbeat []
  {:pool (java.util.concurrent.Executors/newScheduledThreadPool 3)})

(defn create-server+ [id peers rpc]
  (let [message-out (agent rpc)
        server-agt (agent (create-server id peers message-out))]
    {:message-out message-out
     :server-agt server-agt
     :pump (create-message-in rpc server-agt)
     :heartbeat (create-heartbeat)}))

(def heartbeat-period-ms 50)

(defn start-time-things [server+]
  (.start (:pump (:pump server+)))
  (assoc-in server+
            [:heartbeat :task]
            (.scheduleAtFixedRate (:pool (:heartbeat server+))
                                  (partial heartbeat (:server-agt server+))
                                  0 heartbeat-period-ms TimeUnit/MILLISECONDS)))

(defn stop-time-things [server+]
  (swap! (-> server+ :pump :switch) not)
  (.cancel (-> server+ :heartbeat :task) true))

(defn user-command [server+ command]
  (send (:server-agt server+) receive* :user-command {:command command}))

(defn create-servers []
  (let [peer-ids [1 2 3]
        peer-infos (map (partial hash-map :id) peer-ids)
        rpc (create-in-memory-rpc peer-ids)]
    (mapv #(create-server+ % peer-infos rpc) peer-ids)))
