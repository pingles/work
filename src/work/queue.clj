(ns work.queue
  (:refer-clojure :exclude [peek])
  (:import (java.util.concurrent LinkedBlockingQueue
                                 TimeUnit)))

(defn local-queue
  ([]
     (LinkedBlockingQueue.))
  ([xs]
     (LinkedBlockingQueue. xs)))

(defn offer [q v]
  (if-let [r (.offer q v)]
    r
    (throw (Exception. "Queue offer failed."))))

(defn offer-all [q vs]
  (doseq [v vs]
    (offer q v)))

(defn offer-unique
  [q v]
  (if (not (.contains q v))
    (offer q v)))

(defn offer-all-unique [q vs]
  (doseq [v vs]
    (offer-unique q v)))

(defn peek [q] (.peek q))
(defn poll
  ([q] (.poll q))
  ([q t] (.poll q t TimeUnit/SECONDS)))

(defn size [q] (.size q))
