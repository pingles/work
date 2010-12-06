(ns work.queue-test
  (:use clojure.test
	[plumbing.core :only [retry]])
 (:require [work.queue :as work]))

(defn- basic-queue-test [q]
  (work/offer q "b")
  (work/offer-unique q "a")
  (is (= 2 (count q)))
  (is (= (work/peek q) "b"))
  (is (= (work/poll q) "b")))



(deftest local-queue-test
  (basic-queue-test (work/local-queue)))

(deftest local-queue-with-serializer-test
  (basic-queue-test (work/with-serialize (work/local-queue))))
