(ns work.core-test
  (:use clojure.test
	[plumbing.core :only [retry]])
 (:require [work.core :as work])
 (:require [work.message :as msg])
 (:require [work.queue :as q]))

(deftest map-work-test
  (is (= (range 10 1010 10)
	 (work/map-work #(* 10 %)
			10
			(range 1 101 1)
			))))

(defn wait-for-complete-results
  "Test helper fn waits until the pool finishes processing before returning results."
  [response-q expected-seq-size]
  (while (< (.size response-q) expected-seq-size)
    (Thread/sleep 100))
  (sort (iterator-seq (.iterator response-q))))

(deftest do-work-test
  (let [input-data (range 1 101 1)
	response-q (q/local-queue)
	pool (work/do-work #(q/offer response-q (* 10 %))
			   		   10
		   input-data)]
  (is (= (range 10 1010 10)
	 (wait-for-complete-results response-q (count input-data))))))

(deftest queue-work-test
  (let [input-data (range 1 101 1)
	request-q (q/local-queue input-data)
	response-q (q/local-queue)
	pool (future
	      (work/queue-work
	       #(* 10 %)
	       #(q/poll request-q)
	       #(q/offer response-q %)
	       10))]
  (is (= (range 10 1010 10)
         (wait-for-complete-results response-q (count input-data))))))

(deftest blocking-queue-work-test
  (let [input-data (range 1 21 1)
	request-q (q/local-queue input-data)
	response-q (q/local-queue)
	pool (future
	      (work/queue-work
	       #(do (Thread/sleep 1000) (* 10 %))
	       #(q/poll request-q)
	       #(q/offer response-q %)
	       10))]
  (is (= (range 10 210 10)
         (wait-for-complete-results response-q (count input-data))))))

(deftest async-queue-work-test
  (let [input-data (range 1 101 1)
	request-q (q/local-queue input-data)
        response-q (q/local-queue)
        pool (future
              (work/queue-work
               (fn [task put-done]
                 (put-done (* 10 task)))
               #(q/poll request-q)
               #(q/offer response-q %)
               10
               :async))]
    (is (= (range 10 1010 10)
           (wait-for-complete-results response-q (count input-data))))))

(deftest aysnc-blocking-queue-work-test
  (let [input-data (range 1 21 1)
	request-q (q/local-queue input-data)
	response-q (q/local-queue)
	pool (future
	      (work/queue-work
           (fn [task put-done]
	        (Thread/sleep 1000)
            (put-done (* 10 task)))
	       #(q/poll request-q)
	       #(q/offer response-q %)
	       10
           :async))]
  (is (= (range 10 210 10)
         (wait-for-complete-results response-q (count input-data))))))

(defn times10 [x] (* 10 x))

(deftest clj-fns-over-the-queue
  (let [input-data (range 1 101 1)
	request-q (q/local-queue (map #(msg/send-clj %1 %2)
					 (repeat #'times10)
					 input-data))
	response-q (q/local-queue)
	pool (future
	      (work/queue-work
	       msg/clj-worker
	       #(q/poll request-q)
	       #(q/offer response-q %) 
	       10))]
    (is (= (range 10 1010 10)
	   (wait-for-complete-results response-q (count input-data))))))

(deftest json-fns-over-the-queue
  (let [input-data (range 1 101 1)
	request-q (q/local-queue (map #(msg/send-json %1 %2)
					 (repeat #'times10)
					 input-data))
	response-q (q/local-queue)
	pool (future
	      (work/queue-work
	       msg/json-worker
	       #(q/poll request-q)
	       #(q/offer response-q %) 
	       10))]
    (is (= (range 10 1010 10)
	   (wait-for-complete-results response-q (count input-data))))))

(deftest async-task-test
  (let [num-done (atom 0)
	done-q (q/local-queue)
	put-done (fn [x]
		   (swap! num-done inc)
		   (.offer done-q x))
	input-q (q/local-queue (map
				   (fn [i]
				     (work/mk-async-task inc [i] put-done))
				   (range 10)))]
    (work/queue-async-work
     #(q/poll input-q)
     3)
    (is (= (range 1 11)
	   (wait-for-complete-results done-q 10)))
    (is (= 10 @num-done))))