(ns work.cache
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

(defn put-res [m f args & exp]
  (let [res (apply f args)
	_ (.put m args (if (not exp) res
			{:at (time/now) :exp exp :val res}))]
    res))

(defn cache
  "wraps a function f in an (optionally) expiring cache memoizer.  Optionally takes expire, a number of seconds before this item will expire from the cache."
  ([f] (cache f :never))
  ([f expire] (cache (cache-map) f expire))
  ([m f expire]
  (fn [& args]
    (let [m (cache-map)
	  cached (contains-key? m args)
	  expires? (not (= :never expire))]
      (cond
       (and cached expires?)
       (let [{at :at  exp :exp v :val} (get m args)]
	 (if (> (time/in-secs (time/interval at (time/now))) exp)
	   (put-res m f args expire)
	   v))
       cached (get m args)
       (and (not cached) expires?)
       (put-res m f args expire)
       :else
	 (let [res (apply f args)
	     _ (put m args res)]
	 res))))))
