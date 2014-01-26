(ns raft.state.register)

;; Maintains a simple state that allows values to be set and updated

(defn create []
  (atom {}))

(defn reset [state]
  (reset! state {}))

(defn process-dispatch [state cmd]
  (first cmd))

(defmulti process process-dispatch)

(defmethod process :set [state cmd]
  (let [[_ cmd] cmd
        {:keys [name to]} cmd]
    (swap! state assoc name to)))

(defmethod process :update [state cmd]
  (let [[_ cmd] cmd
        {:keys [name from to]} cmd]
    (let [current-val (get @state name)]
      (when (not= current-val from)
        (throw (ex-info "Cannot update value, old value does not match"
                        {:name name, :from from, :current-val current-val}))))
    (swap! state assoc name to)))
