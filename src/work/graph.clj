(ns work.graph
  (:require
   [clojure.contrib.logging :as log]
   [work.core :as work]
   [work.queue :as workq]))

(defn table
  "takes kv pairs.
keys can be vectors of keys, matching a fn.
values can be vectors of fns to apply in sequential order."
  [& pairs]
  (apply hash-map (flatten (map (fn [[k v]]
			    (let [f (if (vector? v)
				      (apply juxt v)
				      v)]
			      (if (vector? k)
				(map (fn [ki] [ki f]) k)
				[k f])))
			  (partition 2 2 pairs)))))

(defn dispatch
"takes a dispatch fn and dispatch table(s).
returns a fn taking args, dispatching on args, and applying dispatch fn to args."
  [d & tables]
  (fn [& args]
    (let [table (apply merge tables)
	  f (table (apply d args))]
      (when-not f
          (log/error (format "unable to dispatch on %s." (pr-str args))))
      (apply f args))))

(defn <-
  "creates a directed edge between vertex:output and f:in.
creates a new dispatched output channel f:out from disp and out.
runs in a work pool accouriding to optional exection strategy based on exec." 
  [vertex f disp & [exec]]
  (let [in (:out vertex)
        out (workq/local-queue)]
    {:in in
     :out out
     :pool (future (work/queue-work
		    {:f f
		     :in #(workq/poll in)
		     :out (disp in out)
		     :exec exec}))}))

(defn <<-
  "A root vertex in a fngraph.
schedules work to feed an output channel which will be a directed edge to subscribing vertices."
  [f freq]
  (let [out (workq/local-queue)]
    {:out out
     :pool (future (work/schedule-work
                    (f out)
                    freq))}))
