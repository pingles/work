(ns work.graph-test
  (:require [work.queue :as q])
  (:use clojure.test
	work.graph))

(deftest table-test
  (is (= {:a 1
	  :b 2}
	 (table :a 1 :b 2)))
  (is (= {:a 1
	  :b 2
	  :c 2
	  :d 2}
	 (table :a 1 [:b :c :d] 2)))
  (let [t (table :a 1 :b [#(+ 1 %) #(- % 1)])]
  (is (= {:a 1
	  :b [5 3]}
	 (assoc t :b ((:b t) 4))))))

(deftest dispatch-test
  (is (= 4
	 ((dispatch
	   (fn [a b] (if (and a b) :foo :bar))
	   (table :foo #(+ %1 %2) :bar #(- %1 %2)))
	  2 2))))

;;TODO: copied from core-test, need to factor out to somehwere.
(defn wait-for-complete-results
  "Test helper fn waits until the pool finishes processing before returning results."
  [response-q expected-seq-size]
  (while (< (.size response-q) expected-seq-size)
    (Thread/sleep 100))
  (sort (iterator-seq (.iterator response-q))))

(deftest graph-test
  (let [input-data (range 1 101 1)
        out (-> (new-graph :input-data input-data)
                (add-edge-> (broadcast-node (partial * 10) :threads 2))
                (add-edge (terminal-node inc :id :output))
                run-graph
		terminal-queues)]
    (is (= (map (fn [x] (inc (* 10 x))) (range 1 101 1))
	   (seq (sort (wait-for-complete-results (:output out) (count input-data))))))))