(ns work.aggregators-test
  (:use clojure.test
	clojure.contrib.map-utils
        work.aggregators
	store.api))

(deftest agg-bucket-test
  (let [agg-b (agg-bucket
	       (hashmap-bucket)
	       +maps
	       #(is (= (bucket-get % :k) {:a 1})))]
    (is (= 1  (agg-inc agg-b)))
    (agg agg-b :k {:a 1})
    (Thread/sleep 3000))
  (let [agg-b2 (agg-bucket
	       (hashmap-bucket)
	       +maps
	       #(is (= (into {} (bucket-seq %)) {:a 3})))]
    (is (= 2 (agg-inc agg-b2 2)))
    (agg agg-b2 {:a 1})
    (agg agg-b2 {:a 2})
    (Thread/sleep 3000)))

(deftest map-reduce-test
  (is (= (reduce + (map inc (range 100)))
         (map-reduce inc + 10 0 (range 100)))))