(ns work.cache-test
  (:require [work.cache :as w.cache])
  (:use clojure.test))

(deftest only-args
  (is (= [2 4 5] ((w.cache/only [_ b _ d e])
                  [1 2 3 4 5]))))

(deftest wrong-cache
  (let [m (w.cache/cache-map)
        c (w.cache/cache m 
                         + ;;caching plus fn
                         :never ;;never expires
                         (w.cache/only [a _ c]))] ;;only cache on first and third args
    (is (= 15 (c 4 5 6)))
    (is (w.cache/contains-key? m [4 6]))
    ;;first call cached [4 6]
    ;;next call is now broken becasue middle slot is not 5, new total sould be 20. cache works.
    (is (= 15 (c 4 10 6)))))

(deftest clearing-cache
  (let [m (w.cache/cache-map)
        c (w.cache/cache m 
                         + ;;caching plus fn
                         1 ;;expires in 1 second
                         (w.cache/only [a _ c]))] ;;only cache on first and third args
    (is (= 15 (c 4 5 6)))
    ;;first call cached [4 6]
    ;;next call is now broken becasue middle slot is not 5, new total sould be 20. cache works.
    (is (= 15 (c 4 10 6)))
    ;;cache is expired now
    (Thread/sleep 2000)
    ;;next call is correct
    (is (= 20 (c 4 10 6)))))