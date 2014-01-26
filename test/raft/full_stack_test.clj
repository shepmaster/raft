(ns raft.full-stack-test
  (:require [clojure.test :refer :all]
            [raft.core :as raft]
            [raft.full-stack :refer :all]))

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

(deftest full-stack
  (let [servers (create-servers)
        started (start servers)]
    (try
      (testing "There is only one leader"
        (is (wait-for 300 #(= 1 (count (leaders started))))))
      (let [terms (each-server started :term)]
        (testing "All the servers are on the same term"
          (is (wait-for 300 #(= 1 (count (set terms))))))
        (testing "It only takes one term to establish a leader"
          (is (wait-for 300 #(= 1 (first terms))))))
      (testing "A user command is committed in reasonable time"
        (let [command-finished (raft/user-command* (leader started) :command)]
          (is (deref command-finished 300 false))))
      (finally
        (stop started)))))
