(ns raft.full-stack
  (:require [raft.core :as raft]
            [raft.rpc.in-memory :as in-memory]))

(defn create-servers [command-fn]
  (let [peer-ids [1 2 3]
        rpc (in-memory/create peer-ids)]
    (mapv #(raft/create-server+ % peer-ids rpc command-fn) peer-ids)))

(defn start [servers]
  (mapv raft/start-time-things servers))

(defn stop [servers]
  (mapv raft/stop-time-things servers))

(defn each-server [servers f]
  (map #(-> % :server-agt deref f) servers))

(defn leaders [servers]
  (filter #(raft/leader? (-> % :server-agt deref)) servers))

(defn leader [servers]
  (first (leaders servers)))
