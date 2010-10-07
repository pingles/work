(ns work.cache
  (:refer-clojure :exclude [get])
  (:require [clj-time.core :as time])
  (:import (java.util.concurrent
	    ConcurrentHashMap)))

(defn cache-map
  ([]
     (ConcurrentHashMap.))
  ([xs]
     (ConcurrentHashMap. xs)))

(defn put [m k v]
  (.put m k v))

(defn put-all [m n]
  (.putAll m n))

(defn get [m k]
  (.get m k))

(defn contains-key? [m k]
  (.containsKey m k))

(defn contains-val? [m v]
  (.containsValue m v))

(defn new-keys [m ks]
  (filter #(not (contains-key? m %)) ks))

(defn put-exp
  "behaves like normal put if there is not expire time, otherwise, it will schedule the element of the map to be deleted once the time expires."
  [m k v & args]
  (let [exp (first args)]
  (.put m k (if (not exp) v
		{:at (time/now) :exp exp :val v}))))

(defmacro only [bindings]
  `(let [bdg# (map (fn [bind#] (if (not (= "_" (name bind#)))
				 true false))
		   '~bindings)]
     (fn [args#] (let [~bindings args#]
		   (filter identity (map
				     (fn [v# keep#]
				       (if keep#
					 v# nil))
				     ~bindings
				     bdg#))))))

(defn cache
  "wraps a function f in an (optionally) expiring cache.  Expire is the number of seconds before this item will expire from the cache."
  ([f] (cache f :never))
  ([f expire] (cache (cache-map) f expire))
  ([m f expire] (cache m f expire identity))
  ([m f expire cache-on]
     (fn [& args]
       (let [cache-args (cache-on args) 
	     cached (contains-key? m cache-args)
	     expires? (not (= :never expire))]
	 (cond
	  (and cached expires?)
	  (let [{at :at  exp :exp v :val} (get m cache-args)]
	    (if (> (time/in-secs (time/interval at (time/now))) exp)
	      (let [res (apply f args)
	 	    _ (put-exp m cache-args res expire)]
	 	res)
	      v))
	  cached (get m cache-args)
	  (and (not cached) expires?)
	  (let [res (apply f args)
	 	_ (put-exp m cache-args res expire)]
	    res)
	  :else
	  (let [res (apply f args)
	 	_ (put m cache-args res)]
	    res))))))