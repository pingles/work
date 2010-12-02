(ns work.aggregators-test
  (:use clojure.test
        work.aggregators
	work.core))

(defn commutativie-agg-test [mk-agg]
  (let [sum-agg (mk-agg identity (fnil + 0))
	max-agg (mk-agg identity (fnil max Double/NEGATIVE_INFINITY))
	min-agg (mk-agg identity (fnil min Double/POSITIVE_INFINITY))]
    (is (= (sum-agg (channel-from-seq [1 2 3])) 6))
    (is (= (max-agg (channel-from-seq [1 2 3])) 3))
    (is (= (min-agg (channel-from-seq [1 2 3])) 1))))

(deftest abelian-agg-test
  (commutativie-agg-test abelian-agg))

(deftest ordered-agg-test
  (commutativie-agg-test ordered-agg)
  (let [sub-agg (ordered-agg identity (fnil - 0))]
    (is (= (sub-agg (channel-from-seq [1 2 3])) -4))))
