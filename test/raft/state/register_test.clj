(ns raft.state.register-test
  (:require [clojure.test :refer :all]
            [raft.state.register :refer :all]))

(def state (create))
(use-fixtures :each (fn [f] (reset state) (f)))

(deftest setting-values-from-nothing
  (testing "Can set a value from nothing"
    (do
      (process state [:set {:name :a, :to 3}]))
    (is (= 3 (:a @state)))))

(deftest setting-values-from-something
  (testing "Can set a value from something"
    (do
      (process state [:set {:name :a, :to 3}])
      (process state [:set {:name :a, :to 4}]))
    (is (= 4 (:a @state)))))

(deftest updating-values
  (testing "Can update a value from nothing"
    (do
      (process state [:update {:name :a, :from nil, :to 3}]))
    (is (= 3 (:a @state)))))

(deftest updating-values-from-something
  (testing "Can update a value from something"
    (do
      (process state [:update {:name :a, :to 3}])
      (process state [:update {:name :a, :from 3, :to 4}]))
    (is (= 4 (:a @state)))))

(deftest updating-values-when-mismatched
  (testing "Updating a value throws an error when the previous value has changed"
    (do
      (process state [:update {:name :a, :to 3}]))
    (is (thrown? clojure.lang.ExceptionInfo
                 (process state [:update {:name :a, :from 0, :to 4}])))))
