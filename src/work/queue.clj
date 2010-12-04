(ns work.queue
  (:refer-clojure :exclude [peek])
  (:use work.message)
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

(defn offer-msg [q & args]
 (let [x (if (= (count args) 1)
	   (first args)
	   args)]
  (offer q (to-msg x))))

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
  [q & [t]]
  (if-let [v 
	   (if t
	     (.poll q t TimeUnit/SECONDS)
	     (.poll q))]
    v nil))

(defn poll-msg [q & [timeout]]
  (from-msg (poll q)))

(defn size [q] (.size q))
