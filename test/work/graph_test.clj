(ns work.graph-test
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