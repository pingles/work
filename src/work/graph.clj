(ns work.graph
  (:require [clojure.contrib.logging :as log]))


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