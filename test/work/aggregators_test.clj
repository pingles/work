(ns work.aggregators-test
  (:use clojure.test
	clojure.contrib.map-utils
        work.aggregators
	store.api))

(deftest with-flush-test
  (let [b (with-merge (hashmap-bucket) (fn [_ x y] y))
	[b-flush pool] (with-flush b  (constantly true) 1)]			 
    (bucket-put b :k :v)
    (Thread/sleep 3000)
    (is (= (bucket-get b :k) :v))
    (is (= (bucket-get b-flush :k) :v))))

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