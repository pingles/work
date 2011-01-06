(ns work.graph
  (:require
   [clojure.contrib.logging :as log]
   [clojure.zip :as zip]
   [work.core :as work]
   [clojure.contrib.zip-filter :as zf]
   [work.queue :as workq])
  (:use    [plumbing.core ]))

(defn table
  "takes kv pairs.
keys can be vectors of keys, matching a fn.
values can be vectors of fns to apply in sequential order."
  [& pairs]
  (apply hash-map (flatten (map (fn [[k v]]
			    (let [f (if (vector? v)
				      (apply juxt v)
				      v)]
			      (if (vector? k)
				(map (fn [ki] [ki f]) k)
				[k f])))
			  (partition 2 2 pairs)))))

(defn dispatch
"takes a dispatch fn and dispatch table(s).
returns a fn taking args, dispatching on args, and applying dispatch fn to args."
  [d & tables]
  (let [table (apply merge tables)]
    (fn [& args]
      (let [f (table (apply d args))]	    
	(when-not f
          (log/error (format "unable to dispatch on %s." (pr-str args))))
	(apply f args)))))

;; Core Vertex Protocols

(defprotocol Inbox
  (receive-message [this src msg] "notifcation of msg from src")
  (poll-message [this] "get message from inbox. blocking. returns :eof when done"))

(defprotocol Outbox
  (broadcast [this src x]
    "broadcast output x to neighbors or for side-effect")
  (add-listener [this listener]
     "return new Outbox with another listener.
      OPTIONAL."))

;;  New Graph Stuff

(defrecord Vertex [f inbox outbox])

(defrecord Edge [when convert-task to])

(defn- mk-edge
  [to &
   {:keys [when, convert-task]
    :or {when (constantly true)
	 convert-task (fn [x] [x])}}]
  (Edge. when convert-task to))

(defn drain-to-vertex
  "send seq to vertex inbox, returns vertex"
  [vertex xs]
  (doseq [x xs] (receive-message (:inbox vertex) :outside x))
  vertex)

;; Inbox implementations

(defrecord  InboxQueue [q]
  Inbox
  (receive-message [this src msg] (workq/offer q msg))
  (poll-message [this] (workq/poll q)))

;; Outbox Impls

(defrecord  DefaultOutbox [out-edges]
  Outbox
  (broadcast [this src x]
	     (doseq [{:keys [when,convert-task,to]} out-edges
		     :when (when x)
		     task (convert-task x)]
	       (cond
		(= to :recur) (receive-message (:inbox src) src task)
		(fn? to) (to task)
		(instance? Vertex to) (receive-message (:inbox to) src task))))
  (add-listener [this listener] (update-in this [:out-edges] conj listener)))

(defrecord  TerminalOutbox [out]
  Outbox
  (broadcast [this src x]
     (workq/offer out x)))

(defn- kill-vertex
  [vertex]
  (-?> vertex :pool deref work/two-phase-shutdown))

(defn- run-vertex
  "launch vertex return vertex with :pool field"
  [{:keys [f,inbox,outbox,threads,sleep-time,exec,make-tasks]
    :or {threads (work/available-processors)
	 sleep-time 10
	 exec work/sync}
     :as vertex}]
  (when (instance? Vertex vertex)
    (let [meter {:num-in-tasks (atom 0)
		 :num-err-tasks (atom 0)
		 :num-out-tasks (atom 0)}
	  
	  args {:f (with-ex
		     (fn [e f args]
		       ((logger) e f args) 
		       (swap! (:num-err-tasks meter) inc))
		     f)
		:in (with-log
		      (fn [& args]		       
		       (let [input (apply poll-message inbox args)]
			 (when (and input (not= input :eof))
			   (swap! (:num-in-tasks meter) inc))
			 input)))		    	
		:out (with-log
		       (fn [x]
			(swap! (:num-out-tasks meter) inc)
			(doseq [task (make-tasks x)]
			  (broadcast outbox vertex task))))
		:sleep-time sleep-time
		:threads threads
		:exec exec}]
      (assoc vertex
	:meter meter
	:pool (future (work/queue-work args))))))
		   
(defn node
  "make a node. only required argument is the fn f at the vertex.
   you can optionally pass in inbox, process, id, or outbox

   Defaults
   --------
   id: gensymd 
   inbox: InboxQueue with new empty queue
   outbox: DefaultOutbox no neighbors
   make-tasks: Make tasks for all children from fn output. Default
   to just output of node function"
  [f  &
   {:keys [id inbox outbox make-tasks]
    :or {inbox (InboxQueue. (workq/local-queue))
         outbox (DefaultOutbox. [])
	 make-tasks (fn [x] [x])
         id (gensym)}
    :as opts}]
  (-> f
      (Vertex. inbox outbox)
      (merge opts)
      (assoc :id id :make-tasks make-tasks)))

(defn terminal-node
  "make a terminal outbox node. same arguments as node
   except that you can pass an :out argument for the queue
   you want the vertex to drain to"
  [& {:keys [f, out] :as opts
      :or {f identity
	   out (workq/local-queue)}}]
  (apply node f
         :outbox (TerminalOutbox. out)
         (apply concat (seq opts))))

(defn- root-node
  "make the root node. same as node except :input-data argument
   lets you pass data to the node's inbox directly"
  [& {:keys [input-data]  :as opts}]
  (apply node identity
           :id :root
           :inbox (InboxQueue. (if input-data
                     (workq/local-queue input-data)
                     (workq/local-queue)))
           (apply concat  (seq opts))))

(defn- add-edge-internal
  [src out-edge]
  (update-in src [:outbox :out-edges] conj out-edge))

(defn graph-zip
  "make a zipper out of a graph. each zipper location is either
   a graph node (Vertex, fn, or keyword) or a graph edge. So to move from
   a node to its parent you zip/up twice for instance. "
  [root]
  (zip/zipper
   ;; branch?
   (fn [x]
     (or (instance? Edge x)
	 (and (instance? Vertex x) (->> x :outbox (instance? DefaultOutbox)))))
   ;; children
   (fn [x]
     (cond
        (instance? Edge x) [(:to x)]
	(instance? Vertex x) (-> x :outbox :out-edges)
	:default (throw (RuntimeException. (format "Can't take children of %s" (pr-str x))))))
   ;; make-node
   (fn [x cs]
     (cond
      (instance? Edge x)
        (do
	  (assert (and (= (count cs) 1)))
	  (assoc x :to (first cs)))
      (instance? Vertex x)
        (do
	  (every? (partial instance? Edge) cs)
	  (assoc-in x [:outbox :out-edges] cs))
	:default (throw (RuntimeException.
			 (format "Can't make node from %s and %s" x cs)))))
   ; root 
   root))

(defn new-graph
  "construct a new graph (just the root vertex). Use :input-data
   optional arg pass in data. Root vertex gets :id of :root.

   Returns graph zipper lcoation"
  [& {:keys [input-data] :as opts}]
  (graph-zip (apply root-node (apply concat (seq opts)))))

(defn add-edge
  "Add edge to current broadcast node in graph-loc. Does
   not change zipper location.

   trg: Either Vertex, :recur or fn

   message is passd to Vertex 
   fn is executed on messgage
   :recur message sent back to vertex
   
   Edge options
   ===============
   :when predicate returning when message is sent to edge
   :convert-task a fn that takes an input task and returns a seq
   of tasks to put in inbox. Default to singleton of input task
   (i.e., no convertion)"
  [graph-loc trg & opts]
  (zip/edit graph-loc add-edge-internal (apply mk-edge trg opts)))

(defn add-edge->
  "same as add-edge but also moves zipper cursor to new trg"
  [graph-loc trg & opts]
  (assert (instance? Vertex (zip/node graph-loc)))
  (-> graph-loc
      (zip/edit add-edge-internal (apply mk-edge trg opts))
      zip/down
      zip/rightmost
      zip/down))

(defn- all-vertices [root]
  (for [loc  (zf/descendants (graph-zip root))
	:when (and loc (->> loc zip/node (instance? Vertex)))]
    (zip/node loc)))

(defn meter-graph
  "return a map from vertex id to a pair
   [num-tasks secs]
   where num-tasks is the number of tasks processed
   and secs is the # of seconds spent processing."
  [root]
  (->> (all-vertices root)
       (map (fn [{:keys [id, meter]}] [id (map-map deref meter)]) )
       (into {})))

(defn run-graph
  "launches in DFS order the vertex processs and returns a map from
   terminal node ids (possibly gensymd) to their out-queues"
  [graph-loc]
  (let [root (zip/root graph-loc)
	update (fn [loc]
		 (if (instance? Vertex (zip/node loc))
		   (zip/edit loc run-vertex)
		   loc))]
    (loop [loc (graph-zip root)]
      (if (zip/end? loc)
	(zip/root loc)
	(recur (-> loc update zip/next))))))

(defn kill-graph [root & to-exclude]
  (let [to-exclude (into #{} to-exclude)]
    (doseq [n (-> root all-vertices)
	    :when (not (to-exclude (:id n)))]
      (kill-vertex n))))

(defn terminal-queues [root]
  (reduce
   (fn [res v]
     (assoc res (:id v) (-> v :outbox :out)))
   {}
   (filter #(instance? TerminalOutbox (:outbox %))
	   (all-vertices root))))