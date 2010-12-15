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

(deftest queue-start-vertex-test
  (let [pool (<<- (fn [out] #(q/offer-all out [:a :b :c]))
                  600)]
    (is (= [:a :b :c]
           (wait-for-complete-results (:out pool) 3)))))

(deftest queue-vertex-test
  (let [input-data (range 1 101 1)
        head-vertex  {:out (q/local-queue input-data)}
        pool (<- head-vertex
                 #(* 10 %)
                 (fn [in out] #(q/offer out %)))]
    (is (= (range 10 1010 10)
           (wait-for-complete-results (:out pool) (count input-data))))))