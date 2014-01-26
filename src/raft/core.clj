(ns raft.core
  (:import (java.util.concurrent TimeUnit))
  (:require [raft.rpc :as rpc]))

(defrecord RPCSend [to name args])
(defrecord RPCReply [name args])
(defrecord RPCBroadcast [name args])
(defrecord LogEntryAdded [log-idx added-fn])
(defrecord LogEntryCommitted [log-idx])

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

(defn other-peer-ids
  [server]
  (remove #{(:id server)} (:peers server)))

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
       :log (vec log))))

(defn log-last-term [server]
  (:term (last (:log server)) no-entries-log-term))

(defn log-length [server]
  (count (:log server)))

(defn log-last-entry-index [server]
  (dec (log-length server)))

(defn log-at-index [server idx]
  (nth (:log server) idx nil))

(defn log-at-index-is-term? [server idx term]
  (if-let [entry (log-at-index server idx)]
    (= term (:term entry))))

(defn log-at-index-is-not-term? [server idx term]
  (if-let [entry (log-at-index server idx)]
    (not= term (:term entry))))

(defn log-remove-last-entry [server]
  (update-in server [:log] butlast))

(defn log-add-entries [server entries]
  (update-in server [:log] into entries))

(defn log-add-new-entry [server command]
  (log-add-entries server [{:term (:term server),
                            :command command}]))

(defn log-entries-at-index [server idx]
  (take 1 (drop idx (:log server))))

(defn log-command-at-index [server idx]
  (-> server :log (get idx) :command))

(defn rand-int-in
  "Creates a random integer between the lower (inclusive) and upper
  (exclusive) bounds"
  [min max]
  (+ min (rand-int (- max min))))

(def election-timeout-ms-min 150)
(def election-timeout-ms-max 300)

(defn create-server
  "peers must be a sequence of connection ids. This server should be
  included in peers"
  [id peers]
  (-> {:id id
       :state :follower
       :peers (set peers)
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

(defn create-request-vote [server]
  (-> (create-rpc-args server)
      (assoc :last-log-term (log-last-term server)
             :last-log-index (log-length server))))

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

(defn peer-map
  "Creates a map from each peer-id to a constant value."
  [server v]
  (zipmap (:peers server) (repeat v)))

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

;; Message handlers

(defn handle-timeout [server _]
  (if (leader? server)
    {:server server}
    (let [server (become-candidate server)]
      {:server server
       :side-effects [(->RPCBroadcast :request-vote
                                      (create-request-vote server))]})))

(defn vote-request-current? [server request-last-log-term request-last-log-index]
  (or
   (> request-last-log-term (log-last-term server))
   (and
    (= request-last-log-term (log-last-term server))
    (>= request-last-log-index (log-length server)))))

(defn handle-request-vote [server args]
  (let [{:keys [sender last-log-term last-log-index]} args
        server (if (vote-request-current? server last-log-term last-log-index)
                 (vote-for server sender)
                 server)]
    {:server server
     :side-effects [(->RPCReply :request-vote-response
                                (create-request-vote-response server sender))]}))

(defn handle-request-vote-response [server args]
  (let [{:keys [sender vote-granted]} args]
    {:server (change-state-from-request-vote-response server sender vote-granted)}))

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

(defn discriminate-append-entries-request [server args]
  (if (accept-append-entries? server args)
    (let [idx (inc (:previous-log-index args))
          entry (first (:entries args))]
      (cond
       (or (heartbeat? args)
           (already-processed? server entry idx))
       :commit

       (conflict? server entry idx)
       :conflict

       (= (log-length server) idx)
       :no-conflict

       :else
       (throw (Exception. "Unknown case"))))
    :rejected))

(defn update-commit-index [server commit-index]
  (assoc server :commit-index commit-index))

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

(defmulti handle-append-entries discriminate-append-entries-request)

(defmethod handle-append-entries :commit [server args]
  {:server (update-commit-index server (:commit-index args))
   :side-effects [(create-accept-append-request server args)]})

(defmethod handle-append-entries :conflict [server args]
  {:server (log-remove-last-entry server)})

(defmethod handle-append-entries :no-conflict [server args]
  (let [entries (take 1 (:entries args))]
    {:server (log-add-entries server entries)}))

(defmethod handle-append-entries :rejected [server args]
  {:server server
   :side-effects [(create-reject-append-request server)]})

(defn create-append-entries-request [server peer-id]
  (let [next-index (get-in server [:next-index peer-id] first-log-entry-index)
        previous-index (dec next-index)
        entry-index (min (log-length server) next-index)]
    (-> (create-rpc-args server)
        (assoc :previous-log-index previous-index
               :previous-log-term (if (no-entries-log-index? previous-index)
                                    no-entries-log-term
                                    (:term (log-at-index server previous-index)))
               :entries (log-entries-at-index server entry-index)))))

(defn quorum-last-agreed-index
  "Returns the smallest log index that a quorum of the servers
  (including this one, the leader) agree on"
  [server]
  (let [index-vals (-> (:last-agreed-index server)
                       (assoc (:id server) (log-length server))
                       (vals))
        quorum-size (majority-count server)]
    (first (take-last quorum-size (sort index-vals)))))

(defn leader-last-agreed-commit-index [server]
  (let [last-agreed-index (quorum-last-agreed-index server)]
    (if (log-at-index-is-term? server last-agreed-index (:term server))
      last-agreed-index
      (:commit-index server))))

(defn append-entries-accepted? [last-agreed-index]
  (> last-agreed-index no-entries-log-index))

(defn update-leader-commit-index [server]
  (update-commit-index server (leader-last-agreed-commit-index server)))

(defn record-append-entries-acceptance [server peer-id last-agreed-index]
  (-> server
      (assoc-in [:next-index peer-id] (inc last-agreed-index))
      (assoc-in [:last-agreed-index peer-id] last-agreed-index)))

(defn record-append-entries-rejection [server peer-id]
  (update-in server [:next-index peer-id] #(max (dec %)
                                                first-log-entry-index)))

(defn handle-append-entries-response [server args]
  (let [{:keys [sender last-agreed-index]} args]
    (if (append-entries-accepted? last-agreed-index)
      (let [server (-> server
                       (record-append-entries-acceptance sender last-agreed-index)
                       (update-leader-commit-index))
            commit-index (:commit-index server no-entries-log-index)]
        {:server server
         :side-effects (if (not= no-entries-log-index commit-index)
                         [(->LogEntryCommitted commit-index)])})
      {:server (record-append-entries-rejection server sender)})))

(defn create-all-append-entries-requests [server]
  (map (fn [id] (->RPCSend id :append-entries (create-append-entries-request server id)))
       (other-peer-ids server)))

;; TODO: assert only leader
(defn handle-user-command [server args]
  (let [{:keys [command added-fn]} args
        server (log-add-new-entry server command)]
    {:server server
     :side-effects (conj (create-all-append-entries-requests server)
                         (->LogEntryAdded (log-last-entry-index server) added-fn))}))

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

(defn receive* [server rpc committed-fn name args]
  (let [{:keys [server side-effects]} (receive-pure server name args)]
    (doseq [side-effect side-effects]
      (let [{:keys [sender]} args
            {:keys [name args]} side-effect
            {:keys [log-idx added-fn]} side-effect]
        (condp = (class side-effect)
          RPCBroadcast (send-off rpc rpc/broadcast (other-peer-ids server) name args)
          RPCReply (send-off rpc rpc/send sender name args)
          RPCSend (send-off rpc rpc/send (:to side-effect) name args)
          LogEntryAdded (added-fn log-idx)
          LogEntryCommitted (committed-fn log-idx (log-command-at-index server log-idx)))))
    server))

;; TODO: unify the create-* RPC functions w.r.t. how they create records

;; TODO: check all state precondition things

;; TODO: discard out-of date messages

;; TODO: assert that the log always stays a vector

;; User commands

(defn message-pump [switch recv process-message]
  (when @switch
    (if-let [[name args] (recv)]
      (process-message name args)
      (process-message :timeout nil))
    (recur switch recv process-message)))

(defn create-message-in [rpc peer-id process-message]
  (let [switch (atom true)
        timeout-ms (rand-int-in election-timeout-ms-min election-timeout-ms-max)
        recv (partial rpc/receive rpc peer-id timeout-ms TimeUnit/MILLISECONDS)]
    {:switch switch
     :timeout-ms timeout-ms
     :pump (Thread. (partial message-pump switch recv process-message))}))

(defn heartbeat [process-message]
  (process-message :heartbeat nil))

(defn create-heartbeat []
  {:pool (java.util.concurrent.Executors/newScheduledThreadPool 3)})

(defn added-to-log [server+ idx-committed log-idx]
  (let [{:keys [inflight]} server+]
    (swap! inflight assoc log-idx idx-committed)))

(defn committed-to-log [command-fn inflight log-idx command]
  (command-fn command)
  (when-let [idx-committed (get @inflight log-idx)]
    (deliver idx-committed true)
    (swap! inflight dissoc log-idx)))

(defn create-server+ [id peers rpc command-fn]
  (let [message-out (agent rpc)
        server-agt (agent (create-server id peers))
        inflight (atom {})
        committed-fn (partial committed-to-log command-fn inflight)
        process-message (partial send server-agt receive* message-out committed-fn)]
    {:process-message process-message
     :message-out message-out
     :server-agt server-agt
     :message-in (create-message-in rpc id process-message)
     :heartbeat (create-heartbeat)
     :inflight inflight}))

(def heartbeat-period-ms 50)

(defn start-time-things [server+]
  (.start (-> server+ :message-in :pump))
  (assoc-in server+
            [:heartbeat :task]
            (.scheduleAtFixedRate (-> server+ :heartbeat :pool)
                                  (partial heartbeat (:process-message server+))
                                  0 heartbeat-period-ms TimeUnit/MILLISECONDS)))

(defn stop-time-things [server+]
  (swap! (-> server+ :message-in :switch) not)
  (.cancel (-> server+ :heartbeat :task) true))

(defn user-command* [server+ command]
  (let [{:keys [process-message]} server+
        idx-committed (promise)
        added-fn (partial added-to-log server+ idx-committed)]
    (process-message :user-command {:command command, :added-fn added-fn})
    idx-committed))

(defn user-command [server+ command]
  @(user-command* server+ command))
