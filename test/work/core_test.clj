(ns work.core-test
 (:use clojure.test)
 (:require [work.core :as work]))

(deftest successfull
  (is (= 10
    (work/retry 5 + 4 6))))

(deftest failure
  (is (= {:fail 1}
    (work/retry 5 / 4 0))))

(defn foo [] 1)

(deftest var-roundtrip
  (is (= 1
	 ((apply work/to-var (work/from-var #'foo)))))) 

(defn add [& args] (apply + args))

(deftest send-and-recieve-clj
  (is (= 6
	 (eval (work/recieve-clj (work/send-clj #'add 1 2 3))))))

(deftest send-and-recieve-json
  (is (= 6
	 (eval (work/recieve-json (work/send-json #'add 1 2 3))))))

(deftest local-queue-test
  (let [q (work/local-queue ["a"])
	_ (work/offer q "b")
	_ (work/offer-unique q "a")]
    (is (= 2 (work/size q)))))

(deftest map-work-test
  (is (= (range 10 1010 10)
	 (work/map-work #(* 10 %)
		   (range 1 101 1)
		   10))))

(defn wait-for-complete-results
  "Test helper fn waits until the pool finishes processing before returning results."
  [response-q expected-seq-size]
  (while (< (.size response-q) expected-seq-size)
    (Thread/sleep 100))
  (sort (iterator-seq (.iterator response-q))))

(deftest do-work-test
  (let [input-data (range 1 101 1)
	response-q (work/local-queue)
	pool (work/do-work #(work/offer response-q (* 10 %))
		   input-data
		   10)]
  (is (= (range 10 1010 10)
	 (wait-for-complete-results response-q (count input-data))))))

(deftest queue-work-test
  (let [input-data (range 1 101 1)
	request-q (work/local-queue input-data)
	response-q (work/local-queue)
	pool (future
	      (work/queue-work
	       #(* 10 %)
	       #(work/poll request-q)
	       #(work/offer response-q %)
	       10))]
  (is (= (range 10 1010 10)
         (wait-for-complete-results response-q (count input-data))))))

(deftest blocking-queue-work-test
  (let [input-data (range 1 21 1)
	request-q (work/local-queue input-data)
	response-q (work/local-queue)
	pool (future
	      (work/queue-work
	       #(do (Thread/sleep 1000) (* 10 %))
	       #(work/poll request-q)
	       #(work/offer response-q %)
	       10))]
  (is (= (range 10 210 10)
         (wait-for-complete-results response-q (count input-data))))))

(deftest async-queue-work-test
  (let [input-data (range 1 101 1)
	request-q (work/local-queue input-data)
        response-q (work/local-queue)
        pool (future
              (work/queue-work
               (fn [task put-done]
                 (put-done (* 10 task)))
               #(work/poll request-q)
               #(work/offer response-q %)
               10
               :async))]
    (is (= (range 10 1010 10)
           (wait-for-complete-results response-q (count input-data))))))

(deftest aysnc-blocking-queue-work-test
  (let [input-data (range 1 21 1)
	request-q (work/local-queue input-data)
	response-q (work/local-queue)
	pool (future
	      (work/queue-work
           (fn [task put-done]
	        (Thread/sleep 1000)
            (put-done (* 10 task)))
	       #(work/poll request-q)
	       #(work/offer response-q %)
	       10
           :async))]
  (is (= (range 10 210 10)
         (wait-for-complete-results response-q (count input-data))))))

(defn times10 [x] (* 10 x))

(deftest clj-fns-over-the-queue
  (let [input-data (range 1 101 1)
	request-q (work/local-queue (map #(work/send-clj %1 %2)
					 (repeat #'times10)
					 input-data))
	response-q (work/local-queue)
	pool (future
	      (work/queue-work
	       work/clj-worker
	       #(work/poll request-q)
	       #(work/offer response-q %) 
	       10))]
    (is (= (range 10 1010 10)
	   (wait-for-complete-results response-q (count input-data))))))

(deftest json-fns-over-the-queue
  (let [input-data (range 1 101 1)
	request-q (work/local-queue (map #(work/send-json %1 %2)
					 (repeat #'times10)
					 input-data))
	response-q (work/local-queue)
	pool (future
	      (work/queue-work
	       work/json-worker
	       #(work/poll request-q)
	       #(work/offer response-q %) 
	       10))]
    (is (= (range 10 1010 10)
	   (wait-for-complete-results response-q (count input-data))))))
