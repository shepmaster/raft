(ns raft.rpc.in-memory
  (:import (java.util.concurrent LinkedBlockingQueue))
  (:require [raft.rpc :as rpc]))

(defrecord InMemoryRPC [sockets]
  rpc/Out
  (send [this peer-id name args]
    (let [socket (get sockets peer-id)]
      (.put socket [name args]))
    this)

  (broadcast [this peer-ids name args]
    (doseq [peer-id peer-ids]
      (rpc/send this peer-id name args))
    this)

  rpc/In
  (receive [this peer-id timeout timeout-units]
    (let [socket (get sockets peer-id)]
      (.poll socket timeout timeout-units))))

(defn create [peer-ids]
  (->InMemoryRPC (zipmap peer-ids (repeatedly #(LinkedBlockingQueue.)))))
