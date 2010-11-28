(ns work.queue-test
  (:use clojure.test
	[plumbing.core :only [retry]])
 (:require [work.queue :as work]))

(deftest local-queue-test
  (let [q (work/local-queue ["a"])
	_ (work/offer q "b")
	_ (work/offer-unique q "a")]
    (is (= 2 (work/size q)))))
