(ns work.message-test
  (:use work.message
	clojure.test)

(defn foo [] 1)

(deftest var-roundtrip
  (is (= 1
	 ((apply work/to-var (work/from-var #'foo)))))) 

(defn add [& args] (apply + args))

(deftest send-and-recieve-clj
  (is (= 6
	 (eval (work/recieve-clj (work/send-clj #'add 1 2 3))))))

(deftest send-and-recieve-json
  (is (= 6
	 (eval (work/recieve-json (work/send-json #'add 1 2 3))))))
