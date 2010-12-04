(ns work.queue-test
  (:use clojure.test
	[plumbing.core :only [retry]])
 (:require [work.queue :as work]))

(deftest local-queue-test
  (let [q (work/local-queue ["a"])
	_ (work/offer q "b")
	_ (work/offer-unique q "a")]
    (is (= 2 (work/size q)))))

(deftest message-queue-test
  (let [q (work/local-queue)
	_ (work/offer-msg q [1 2 3])
	_ (work/offer-msg q 3 4 5)]
    (is (= [1 2 3] (work/poll-msg q)))
    (is (= [3 4 5] (work/poll-msg q)))))
