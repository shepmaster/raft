(ns raft.core-test
  (:require [clojure.test :refer :all]
            [simple-check.core :as sc]
            [simple-check.generators :as gen]
            [simple-check.properties :as prop]
            [simple-check.clojure-test :as ct]
            [raft.core :refer :all]))

(deftest initial-state
  (let [server (create-server nil nil)]
    (testing "The server starts out as a follower"
      (is (follower? server)))))

(defn- fake-peers [n]
  (repeat n nil))

(defn fake-log [n]
  (map (fn [n] {:term 0, :command (symbol (str ":" "command" n))}) (range n)))

(deftest focused-tests
  (testing "Becoming a follower"
    (let [server {:state :leader, :term 5}
          follower-server (become-follower server 6)]

      (is (follower? follower-server))
      (is (= 6 (:term follower-server)))
      (is (not (contains? follower-server :voted-for))))

    (testing "Does nothing when the term is not newer"
      (let [server {:state :leader, :term 5}]
        (is (leader? (become-follower server 5)))
        (is (leader? (become-follower server 4))))))

  (testing "Giving a vote only applies once"
    (is (= 1 (:voted-for (vote-for {} 1))))
    (is (= 1 (:voted-for (-> {}
                             (vote-for 1)
                             (vote-for 2))))))

  (testing "Majority count"
    (is (= 1 (majority-count {:peers (fake-peers 1)})))
    (is (= 1 (majority-count {:peers (fake-peers 2)})))
    (is (= 2 (majority-count {:peers (fake-peers 3)})))
    (is (= 2 (majority-count {:peers (fake-peers 4)})))
    (is (= 3 (majority-count {:peers (fake-peers 5)}))))

  (testing "Quorum-wide last-agreed index"
    (is (= 9
           (quorum-last-agreed-index {:peers (fake-peers 1),
                                      :last-agreed-index {},
                                      :log (fake-log 9)})))
    (is (= 8
           (quorum-last-agreed-index {:peers (fake-peers 3),
                                      :last-agreed-index {:1 5, :2 8},
                                      :log (fake-log 9)})))
    (is (= 7
           (quorum-last-agreed-index {:peers (fake-peers 5),
                                      :last-agreed-index {:1 8, :2 6, :3 7, :4 5},
                                      :log (fake-log 9)}))))

  (testing "Becoming a leader"
    (let [leader (become-leader {:state :candidate
                                 :peers (fake-peers 5)
                                 :votes {1 true, 2 true, 3 true}})]
      (is (leader? leader)))

    (testing "Does nothing if not already a candidate"
      (is (follower? (become-leader {:state :follower})))

    (testing "Does nothing if there is no majority"
      (is (candidate? (become-leader {:state :candidate
                                      :peers (fake-peers 5)
                                      :votes {}})))))))

(def gen-peer
  (gen/hash-map :id gen/string-alpha-numeric))
(def gen-peer-set
  (gen/such-that #(odd? (count %)) (gen/vector gen-peer)))
(def gen-server-args
  (gen/bind gen-peer-set
            #(gen/tuple (gen/return (:id (first %))) ;; Better way than first?
                        (gen/return %))))
(def gen-server
  (gen/fmap (partial apply create-server)
            gen-server-args))

(ct/defspec property-based 100
  (testing "all initial servers are followers"
    (prop/for-all [server gen-server]
                  (raft.core/follower? server))))

(defn add-test-log-entries [server & entries]
  (update-in server [:log] concat entries))

(defn add-test-request-entry [request entry]
  (update-in request [:entries] conj entry))

(deftest append-entries-integration
  (let [leader-id 1
        follower-id 2
        entry {:term 1, :command :dummy}
        leader (-> {:id leader-id, :term 1, :next-index {}} (add-test-log-entries entry))
        follower {:id leader-id, :term 1, :log []}]

    (testing "Can send entries from the leader to a follower"
      (let [follower (:server (->> (create-append-entries-request leader follower)
                                   (receive-pure follower :append-entries)))]
        (is (= [entry] (:log follower)))))))

(defn set-next-index [server peer-id idx]
  (assoc-in server [:next-index peer-id] idx))

(deftest creating-append-entries-requests
  (let [peer-id :peer-id
        entry1 {:term 4, :command :dummy1}
        entry2 {:term 5, :command :dummy2}
        entry3 {:term 6, :command :dummy3}
        server (-> {:term 10, :id 99, :next-index {}}
                   (add-test-log-entries entry1 entry2 entry3))
        build-request (fn [server] (create-append-entries-request server peer-id))
        set-next-index (fn [server idx] (set-next-index server peer-id idx))]

    (testing "Bookkeeping"
      (let [request (build-request server)]
        (is (= (:term server) (:term request)))
        (is (= (:id server) (:sender request)))))

    (testing "When we are sending"
      (testing "the first entry"
        (let [request (build-request server)]
          (is (= no-entries-log-index (:previous-log-index request)))
          (is (= no-entries-log-term (:previous-log-term request)))
          (is (= [entry1] (:entries request)))))

      (testing "a middle entry"
        (let [server (set-next-index server 1)
              request (build-request server)]
          (is (= 0 (:previous-log-index request)))
          (is (= (:term entry1) (:previous-log-term request)))
          (is (= [entry2] (:entries request)))))

      (testing "the last entry"
        (let [server (set-next-index server 2)
              request (build-request server)]
          (is (= 1 (:previous-log-index request)))
          (is (= (:term entry2) (:previous-log-term request)))
          (is (= [entry3] (:entries request))))))

    (testing "When we are up-to-date with this peer"
      (let [server (set-next-index server 3)
            request (build-request server)]
        (is (= 2 (:previous-log-index request)))
        (is (= (:term entry3) (:previous-log-term request)))
        (is (empty? (:entries request)))))))

(defn receive-partial [name]
  (fn [server args] (receive-pure server name args)))

(deftest appending-entries
  (let [server {:term 1, :log []}
        request {:term 1, :entries [], :sender 0
                 :previous-log-index no-entries-log-index
                 :previous-log-term no-entries-log-term}
        recv-append (receive-partial :append-entries)]

    (testing "When it is just a heartbeat"
      (let [request (assoc request :commit-index 2)
            {:keys [server side-effects]} (recv-append server request)
            rpc (first side-effects)]
        (is (= (:commit-index request)
               (:commit-index server)))
        (is (= (+ (:previous-log-index request) (count (:entries request)))
               (:last-agreed-index (:args rpc))))))

    (testing "When the request has already been processed"
      ;; add greater than and equal cases
      (let [entry {:term 1, :command :dummy}
            server (add-test-log-entries server entry)
            request (-> request
                        (add-test-request-entry entry)
                        (assoc :commit-index 2))
            {:keys [server side-effects]} (recv-append server request)
            rpc (first side-effects)]
        (is (= (:commit-index request)
               (:commit-index server)))
        (is (= (+ (:previous-log-index request) (count (:entries request)))
               (:last-agreed-index (:args rpc))))))

    (testing "When there are no conflicts"
      ;; add test for log index doesnt match
      (let [request (add-test-request-entry request :dummy-entry)
            old-server server
            {:keys [server side-effect]} (recv-append server request)]
        (is (= (inc (log-length old-server))
               (log-length server)))))

    (testing "When there is a conflict"
      ;; Add test for equal and greater than placement cases
      (testing "Because the updates term does not match our term"
        (let [server (add-test-log-entries server {:term 1, :command :dummy})
              request (add-test-request-entry request {:term 2, :command :dummy2})]
          (is (= (dec (log-length server))
                 (log-length (recv-append server request)))))))

    (testing "When the request is not accepted"
      (testing "Because the message's term is out of date"
        (let [request (assoc request :term (dec (:term server)))
              old-server server
              {:keys [server side-effects]} (recv-append server request)
              rpc (first side-effects)]
          (is (= old-server server))
          (is (= no-entries-log-index
                 (:last-agreed-index (:args rpc)))))))))

(deftest test-accept-append-entries?
  (testing "Not accepted"
    (testing "When term does not match"
      (is (not (accept-append-entries? {:term 0} {:term 1}))))

    (testing "When the previous log entry does not match"
      (is (not (accept-append-entries? {} {:previous-log-index 1})))))

  (testing "Accepted"
    (testing "When there are no previous log entries"
      (is (accept-append-entries? {} {:previous-log-index -1})))

    (testing "When the previous log entry does match"
      (is (accept-append-entries? {:log [{:term 4}]}
                                  {:previous-log-index 0, :previous-log-term 4})))))

(deftest timing-out
  (let [id 42
        server {:id id, :term 0, :log [{:term 3} {:term 7}]}
        recv-timeout (receive-partial :timeout)]
    (testing "When a server times out"
      (let [{:keys [server side-effects]} (recv-timeout server nil)
            rpc (first side-effects)]
        (testing "It becomes a candidate"
          (is (candidate? server)))

        (testing "It votes for itself"
          (is (= id (:voted-for server)))
          (is (= {id true} (:votes server))))

        (testing "It requests votes"
          (is (= :request-vote (:name rpc)))
          (is (= 7 (-> rpc :args :last-log-term)))
          (is (= 2 (-> rpc :args :last-log-index)))))

      (testing "But it is the leader"
        (let [server (init-leader server)
              old-server server
              {:keys [server side-effects]} (recv-timeout server nil)]
          (testing "It does not change"
            (is (= old-server server)))
          (testing "It has no side-effects"
            (is (empty? side-effects))))))))

(deftest vote-requested
  (let [id 42
        server {:id id, :term 10}
        recv-vote-req (receive-partial :request-vote)]
    (testing "When a vote is requested"
      (testing "And we haven't voted for anyone"
        (let [{:keys [server side-effects]} (recv-vote-req server {:sender 1,
                                                                   :last-log-term 20})
              rpc (first side-effects)]
          (testing "It votes for the candidate"
            (is (= :request-vote-response (:name rpc)))
            (is (:vote-granted (:args rpc))))))

      (testing "But we have already voted for someone else"
        (let [{:keys [server]}              (recv-vote-req server {:sender 1,
                                                                   :last-log-term 20})
              {:keys [server side-effects]} (recv-vote-req server {:sender 2,
                                                                   :last-log-term 20})
              rpc (first side-effects)]
          (testing "It does not vote for the candidate"
            (is (= :request-vote-response (:name rpc)))
            (is (not (:vote-granted (:args rpc)))))))

      (testing "But we have already voted for the candidate"
        (let [{:keys [server]}              (recv-vote-req server {:sender 1,
                                                                   :last-log-term 20})
              {:keys [server side-effects]} (recv-vote-req server {:sender 1,
                                                                   :last-log-term 20})
              rpc (first side-effects)]
          (testing "It votes for the candidate"
            (is (= :request-vote-response (:name rpc)))
            (is (:vote-granted (:args rpc))))))

      (testing "But the candidate is not up-to-date"
        (let [server (assoc server :log [{:term 7} {:term 10}])]
          (testing "Because the candidate's log term is old"
            (let [{:keys [server side-effects]} (recv-vote-req server {:sender 1,
                                                                       :last-log-term 5})
                  rpc (first side-effects)]
              (testing "It does not vote for the candidate"
                (is (= :request-vote-response (:name rpc)))
                (is (not (:vote-granted (:args rpc)))))))
          (testing "Because the candidate's log index does not match"
            (let [{:keys [server side-effects]} (recv-vote-req server {:sender 1,
                                                                       :last-log-term 10,
                                                                       :last-log-index 1})
                  rpc (first side-effects)]
              (testing "It does not vote for the candidate"
                (is (= :request-vote-response (:name rpc)))
                (is (not (:vote-granted (:args rpc))))))))))))

(deftest vote-responded
  (let [id 42
        server {:id id, :term 0, :peers [id 2 3 4 5]}
        server (-> server become-candidate)
        recv-vote (receive-partial :request-vote-response)]
    (testing "When a peer responds with a vote"
      (testing "And we do not have the majority of votes"
        (let [{:keys [server]} (recv-vote server {:sender 2, :vote-granted true})]
          (is (candidate? server))))

      (testing "And we have the majority of votes"
        (let [{:keys [server]} (recv-vote server {:sender 2, :vote-granted true})
              {:keys [server]} (recv-vote server {:sender 3, :vote-granted true})]
          ;; Sends out heartbeat!
          (is (leader? server)))))))

(deftest append-entries-responded
  (let [id 42
        peers [1 2 3 4 id]
        peer-id (fn [n] (peers n))
        server (-> {:id id, :term 0, :peers peers}
                   (init-log-vars (fake-log 20))
                   (init-leader)
                   (set-next-index (peer-id 0) 10))
        old-server server
        recv-append-resp (receive-partial :append-entries-response)
        next-index (fn [server peer-n] (get-in server [:next-index (peer-id peer-n)]))
        last-agreed-index (fn [server peer-n] (get-in server [:last-agreed-index (peer-id peer-n)]))]

    (testing "When a peer rejects the entries"
      (let [request {:sender (peer-id 0), :last-agreed-index no-entries-log-index}
            {:keys [server]} (recv-append-resp server request)]
        (testing "It decreases that peer's next index"
          (is (= (dec (next-index old-server 0))
                 (next-index server 0))))
        (testing "but the next index is already at the beginning"
          (let [server (set-next-index server (peer-id 0) first-log-entry-index)
                {:keys [server]} (recv-append-resp server request)]
            (testing "It doesn't decrease it further"
              (is (<= first-log-entry-index (next-index server 0))))))))

    (testing "When a peer accepts the entries"
      (let [request {:sender (peer-id 0), :last-agreed-index 10}
            {:keys [server]} (recv-append-resp server request)]
        (testing "It updates the next index"
          (is (= (inc (:last-agreed-index request))
                 (next-index server 0))))
        (testing "It updates the last-agreed index"
          (is (= (:last-agreed-index request)
                 (last-agreed-index server 0))))))

    (testing "When a majority of the peers accept an index"
      (let [request {:sender (peer-id 0), :last-agreed-index 10}
            {:keys [server]} (recv-append-resp server request)
            request {:sender (peer-id 1), :last-agreed-index 15}
            {:keys [server]} (recv-append-resp server request)]
        (testing "It updates the commit index"
          (is (= 10
                 (:commit-index server))))))))

(defn rpc-side-effects [side-effects]
  (filter #(#{raft.core.RPCSend} (class %)) side-effects))

(deftest user-adds-commands
  (let [id 42
        peers [1 2 3 4 id]
        server {:id id, :term 0, :peers peers}
        old-server server
        recv-user-cmd (receive-partial :user-command)]
    (testing "When a user adds a new command"
      (let [{:keys [server side-effects]} (recv-user-cmd server {:command :dummy})]
        (testing "It adds the command to the log"
          (is (= (inc (log-length old-server))
                 (log-length server))))
        (testing "It sends log updates to its peers"
          (is (= (dec (count (:peers server)))
                 (count (rpc-side-effects side-effects)))))))))

(defn wait-for
  "Wait for a condition to become truthy. Returns `false` if the
  timeout expires."
  [timeout-ms f]
  (if (>= timeout-ms 0)
    (if-let [result (f)]
      result
      (do
        (Thread/sleep 50)
        (recur (- timeout-ms 50) f)))
    false))

(deftest full-scale
  (let [servers (create-servers)
        started (mapv start-time-things servers)]
    (try
      (testing "There is only one leader"
        (is (wait-for 300 #(= 1 (count (leaders started))))))
      (let [terms (each-server started :term)]
        (testing "All the servers are on the same term"
          (is (wait-for 300 #(= 1 (count (set terms))))))
        (testing "It only takes one term to establish a leader"
          (is (wait-for 300 #(= 1 (first terms))))))
      (testing "A user command is committed in reasonable time"
        (let [command-finished (user-command* (leader started) :command)]
          (is (deref command-finished 300 false))))
      (finally
        (mapv stop-time-things started)))))
