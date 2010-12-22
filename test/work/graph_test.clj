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
		 (add-edge (terminal-node inc :id :inc))
		 (add-edge (terminal-node identity :id :identity))
		  run-graph)
	out (terminal-queues root)]
    (is (= (range 1 6) (wait-for-complete-results (:inc out) 5)))
    (is (= (range 5) (wait-for-complete-results (:identity out) 5)))
    (kill-graph root)))

(deftest chain-graph-test
  ; (range 5) -> inc -> inc
  (let [root (-> (new-graph :input-data (range 5))
		 (add-edge-> (node inc))
		 (add-edge (terminal-node inc :id :out))
		 (add-edge (terminal-node (partial + 2) :id :plus-two))
		 run-graph)
	outs (terminal-queues root)]
    (is (= (range 2 7) (wait-for-complete-results (:out outs) 5)))
    (is (= (range 3 8) (wait-for-complete-results (:plus-two outs) 5)))
    (kill-graph root)))

(deftest graph-test
  (let [input-data (range 1 101 1)
        root (-> (new-graph :input-data input-data)
                (add-edge-> (node (partial * 10) :threads 2))
                (add-edge (terminal-node inc :id :output))
                run-graph)
	out (terminal-queues root)]
    (is (= (map (fn [x] (inc (* 10 x))) (range 1 101 1))
	   (wait-for-complete-results (:output out) (count input-data))))
    (kill-graph root)))

;; (deftest dispatch-graph-test
;;   (let [sink (terminal-node identity :id :out)
;; 	root (-> (node identity
;; 		    :outbox (mk-outbox
;; 			     (fn [x]
;; 			       (swank.core/break)
;; 			       (cond (even? x) [:out :even]
;; 				     (odd? x) [:odd]))))
;; 		 graph-zip
;; 		 (add-edge sink)
;; 		 zip/root)
;; 	;; #_g #_(-> (new-graph :input-data (range 100))
;; 	;; 	 (add-edge-> (dispatch-node identity
;; 	;; 		        (fn [x]
;; 	;; 			  (swank.core/break)
;; 	;; 			  (cond (even? x) [:even]
;; 	;; 				(odd? x) [:odd]))))
;; 	;; 	 (add-edge-> (node inc :id :even))
;; 	;; 	 (add-edge sink)
;; 	;; 	 zip/up
;; 	;; 	 (add-edge-> (node (partial + 2) :id :odd))
;; 	;; 	 (add-edge sink)
;; 	;; 	 run-graph)
;; 	]
;;     ((-> root :outbox :disp) 1 nil)))

;; (dispatch-graph-test)