(ns work.message
  (:require [clj-json [core :as json]]
            [clojure.contrib.logging :as log])
  (:use	clj-serializer.core
	[clojure.contrib.def :only [defvar]]
	[plumbing.core :only [silent]])
  (:import clojure.lang.RT))

(defn from-var
  "convert fn variable to [ns-name fn-name] string pair"
  [^Var fn-var]
  (let [m (meta fn-var)]
    [(str (:ns m)) (str (:name m))]))

(defn to-var
  "find variable named by [ns-name fn-name] strings"
  [^String ns-name ^String fn-name]
  (let [root (-> ns-name
		 (.replace "-" "_")
		 (.replace "." "/"))]
    (silent #(RT/load %1) root)
    (.deref (RT/var ns-name, fn-name))))

(defn- recieve*
  "msg should take form [[ns-name fn-name] args]
   and return a list which when eval'd represents
   executing fn on args" 
  [msg]
  (let [[[ns-name fn-name] & args] msg]
    (cons (to-var ns-name fn-name) args)))

(defn from-msg [x]
  (deserialize (.getBytes x) :eof))

(defn recieve-clj
  "receive* message represented as a serialized
   clojure data object"
  [msg]
  (recieve* (from-msg msg)))

(defn recieve-json
  "receive* message presented as a json string"
  [msg]
  (recieve* (json/parse-string msg)))

(defvar clj-worker
  (comp eval recieve-clj)
  "evaluate msg represented by serialized clojure object")

(defvar json-worker
  (comp eval recieve-json)
  "evaluate msg represented by json string")

(defn to-msg [x]
  (String. (serialize x)))

(defn send-clj
  "convert fn evaluation to String representing
   function evaluation as a clojure object message"
  [fn-var & args]
  (-> fn-var
      from-var
      (cons args)
      to-msg))

(defn send-json
  "convert fn evaluation to String json representation"
  [fn-var & args]
  (-> fn-var
      from-var
      (cons args)
      json/generate-string))