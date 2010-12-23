(ns work.graph-test
  (:require [work.queue :as q] [clojure.zip :as zip])
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

(deftest one-node-graph-test
  (let [root (-> (new-graph :input-data (range 5))
		 (add-edge (terminal-node :f inc :id :inc))
		 (add-edge (terminal-node :id :identity))
		  run-graph)
	out (terminal-queues root)]
    (is (= (range 1 6) (wait-for-complete-results (:inc out) 5)))
    (is (= (range 5) (wait-for-complete-results (:identity out) 5)))
    (kill-graph root)))

(deftest chain-graph-test
  ; (range 5) -> inc -> inc
  (let [root (-> (new-graph :input-data (range 5))
		 (add-edge-> (node inc))
		 (add-edge (terminal-node :f inc :id :out))
		 (add-edge (terminal-node :f (partial + 2) :id :plus-two))
		 run-graph)
	outs (terminal-queues root)]
    (is (= (range 2 7) (wait-for-complete-results (:out outs) 5)))
    (is (= (range 3 8) (wait-for-complete-results (:plus-two outs) 5)))
    (kill-graph root)))

(deftest graph-test
  (let [input-data (range 1 101 1)
        root (-> (new-graph :input-data input-data)
                (add-edge-> (node (partial * 10) :threads 2))
                (add-edge (terminal-node :f inc :id :output))
                run-graph)
	out (terminal-queues root)]
    (is (= (map (fn [x] (inc (* 10 x))) (range 1 101 1))
	   (wait-for-complete-results (:output out) (count input-data))))
    (kill-graph root)))

(deftest simple-dispatch-test
  (let [disp (fn [x]
	       (cond (odd? x) [:odd :all :plus-1]
		     (even? x) [:even :all]))
	root (-> (new-graph :input-data [1 2 3 4 5])
		 (add-edge-> (dispatch-node identity disp))
		 (add-edge (terminal-node :id :even))
		 (add-edge (terminal-node :id :odd))
		 (add-edge (terminal-node :id :all))
		 (add-edge (terminal-node :f inc :id :plus-1))
		 run-graph)
	outs (terminal-queues root)]
    (Thread/sleep 2000)
    (kill-graph root)
    (is (= (-> outs :even seq sort) [2 4]))
    (is (= (-> outs :odd seq sort) [1 3 5]))
    (is (= (-> outs :all seq sort) (range 1 6)))
    (is (= (-> outs :plus-1 seq sort) (map inc [1 3 5])))))

(deftest simple-sideffect-test
  (let [a (atom nil)
	root (-> (new-graph :input-data [1 2 3 4 5])		 
		 (add-edge-> (side-effect-node
			      (fn [x] (swap! a conj x))))
		 run-graph)]
    (Thread/sleep 2000)
    (is (= (sort @a) [1 2 3 4 5]))))
