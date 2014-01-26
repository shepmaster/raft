(ns raft.rpc
  (:refer-clojure :exclude [send]))

(defprotocol Out
  (send [this peer-id name args])
  (broadcast [this peer-ids name args]))

(defprotocol In
  (receive [this peer-id timeout timeout-units]))
