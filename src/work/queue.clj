(ns work.queue
  (:refer-clojure :exclude [peek])
  (:use work.message plumbing.core plumbing.serialize)
  (:import (java.util.concurrent LinkedBlockingQueue)))                                 

(defprotocol Queue
  (poll [q] "poll")
  (peek [q] "see top elem without return")
  (offer [q x] "offer"))

(extend-protocol Queue
  LinkedBlockingQueue
  (poll [this] (.poll this))
  (offer [this x]
	 (if-let [r (.offer this x)]
	   r
	   (throw (Exception. "Queue offer failed"))))
  (peek [this] (.peek this)))

(defn with-serialize
  "decorates queue with serializing input and output to and from bytes."
  ([serialize-impl queue]
     (reify Queue
	    (poll [q] (thaw serialize-impl (poll queue)))
	    (offer [q x] (offer queue (freeze serialize-impl x)))
	    (peek [q] (thaw serialize-impl (peek queue)))
	    clojure.lang.Seqable
	    (seq [this] (seq queue))
	    clojure.lang.Counted
	    (count [this] (count queue))))
  ([q] (with-serialize default-serializer q)))

(defn local-queue
  "return LinkedBlockingQueue implementation
   of Queue protocol."
  ([]
     (LinkedBlockingQueue.))
  ([xs]
     (LinkedBlockingQueue. xs)))

(defn offer-all [q vs]
  (doseq [v vs]
    (offer q v)))

(defn offer-unique
  "assumes q is a Queue and Seqable"
  [q v]
  (if (not (find-first (partial = v) q))
    (offer q v)))

(defn offer-all-unique [q vs]
  (doseq [v vs]
    (offer-unique q v)))
