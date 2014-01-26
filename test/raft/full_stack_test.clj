(ns raft.full-stack-test
  (:require [clojure.test :refer :all]
            [raft.core :as raft]
            [raft.state.register :as register]
            [raft.full-stack :refer :all]))

(defn eventually=*
  "Wait for a function to equal the expected value. Returns a tuple of
  :success/:timeout and the actual value."
  [timeout-ms expected f]
  (let [result (f)
        remaining-timeout (- timeout-ms 50)]
    (cond
     (= expected result) [:success result]
     (<= remaining-timeout 0) [:timeout result]
     :else (do
             (Thread/sleep (min remaining-timeout 50))
             (recur remaining-timeout expected f)))))

(defmethod assert-expr 'eventually= [msg form]
  `(let [timeout# ~(nth form 1)
         expected# ~(nth form 2)
         actual-fn# (fn [] ~(nth form 3))]
     (let [[result# actual#] (eventually=* timeout# expected# actual-fn#)]
       (if (= :success result#)
         (do-report {:type :pass,
                     :message ~msg,
                     :expected expected#,
                     :actual actual#})
         (do-report {:type :fail,
                     :message ~msg,
                     :expected expected#,
                     :actual (str actual# " (after waiting " timeout# " ms)")}))
       (= :success result#))))

(deftest full-stack
  (let [state (register/create)
        process (partial register/process state)
        servers (create-servers process)
        started (start servers)]
    (try
      (testing "There is only one leader"
        (is (eventually= 300 1 (count (leaders started)))))
      (let [terms (each-server started :term)]
        (testing "All the servers are on the same term"
          (is (eventually= 300 1 (count (set terms)))))
        (testing "It only takes one term to establish a leader"
          (is (eventually= 300 1 (first terms)))))
      (testing "A user command is committed in reasonable time"
        (let [command-finished (raft/user-command* (leader started) [:set {:name :a, :to 5}])]
          (is (deref command-finished 300 false))
          (is (eventually= 300 5 (:a @state)))))
      (finally
        (stop started)))))
