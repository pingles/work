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

;; Broadcast Policy

; outs is seq of Vertexs
(defrecord  DefaultOutbox [disp outs]
  Outbox
  (broadcast [this src x]
             (doseq [o (disp x outs)]
	       (cond
		(= o :self)  (receive-message (:inbox src) src x)
		(instance? Vertex o) (receive-message (:inbox o) this x)
		(fn? o) (o x))))
  (add-listener [this listener] (update-in this [:outs] conj listener)))

(defrecord  TerminalOutbox [out]
  Outbox
  (broadcast [this src x]
     (workq/offer out x)))

(defn mk-outbox
  ([disp]
     (DefaultOutbox.
       (fn [x outs]
	 (if-not disp
	   outs
	   (let [outs-by-id (group-by :id outs)]
	     (for [t (disp x)
		   d (cond
		      (fn? t) t
		      (= :recur t) :recur
		      (keyword? t) (first (outs-by-id t))
		      :default nil)
		   :when d]
	       d))))
       []))
  ([] (mk-outbox nil)))

;; Queue-Work

;; Start Vertex process put results in :process of vertex

(defn- kill-vertex
  [vertex]
  (-?> vertex :pool deref work/shutdown-now))

(defn- run-vertex
  "launch vertex return vertex with :pool field"
  [{:keys [f,inbox,outbox,threads,sleep-time,exec]
    :or {threads (work/available-processors)
	 sleep-time 1000
	 exec work/sync}
     :as vertex}]
  (let [args {:f f
	      :in (partial poll-message inbox)
	      :out (partial broadcast outbox vertex)
	      :sleep-time sleep-time
	      :threads threads
	      :exec exec}]
    (assoc vertex
     :pool-args (into {} args)
     :pool (future (work/queue-work args)))))
		   
(defn node
  "make a node. only required argument is the fn f at the vertex.
   you can optionally pass in inbox, process, id, or outbox

   Defaults
   --------
   id: gensymd 
   inbox: InboxQueue with new empty queue
   outbox: DefaultOutbox no neighbors"
  [f  &
   {:keys [id inbox outbox process]
    :or {inbox (InboxQueue. (workq/local-queue))
         outbox (mk-outbox)
         id (gensym)}
    :as opts}]
  (-> f
      (Vertex. inbox outbox)
      (assoc :id id)))

(defn dispatch-node
  "make a dispatching node
   f: function for vertex to execute on incoming messages
   disp: function which takes output of (f task) and returns
   a seq of either keywords or fns. Each fn is executed and
   each keyword is assumed to name a neighbor vertex. new element
   is sent to that vertex"
  [f disp & opts]
  (let [new-opts (-> opts flatten (conj [:outbox (mk-outbox disp)]))]
    (apply node f new-opts)))

(defn terminal-node
  "make a terminal outbox node. same arguments as node
   except that you can pass an :out argument for the queue
   you want the vertex to drain to"
  [f & {:keys [out] :as opts}]
  (apply node f
         :outbox (if out (TerminalOutbox. out) (TerminalOutbox. (workq/local-queue)))
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
  [src trg]
  (update-in src [:outbox :outs] conj trg))

(defn graph-zip
  "make a zipper out of a graph"
  [root]
  (zip/zipper
   ; branch?
   (fn [n]
     (->> n :outbox (instance? DefaultOutbox)))
   ; children
   (comp :outs :outbox)
   ; make-node
   (fn [n cs]
     (assoc-in  n [:outbox :outs] cs))
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
   not change zipper location"
  [graph-loc trg]
  (zip/edit graph-loc add-edge-internal trg))

(defn add-edge->
  "same as add-edge but also moves zipper cursor to new trg"
  [graph-loc trg]
  (-> graph-loc (add-edge trg) zip/down zip/rightmost))

(defn- all-vertices [root]
  (remove nil? (map zip/node (zf/descendants (graph-zip root)))))

(defn run-graph
  "launches in DFS order the vertex processs and returns a map from
   terminal node ids (possibly gensymd) to their out-queues"
  [graph-loc]
  (let [root (zip/root graph-loc)]
    (doseq [v (all-vertices root)]
      (run-vertex v))
    root))

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

(comment

  (def n (-> identity
	     (node
	      :inbox (InboxQueue. (workq/local-queue [1 2 3]))
	      :outbox (TerminalOutbox. (workq/local-queue)))
	     run-vertex))
  

  (def x (-> (new-graph :input-data (range 5))
	     (add-edge (terminal-node inc :id :final))
	     run-graph
	     terminal-queues))
  
  
  (def out (-> (new-graph :input-data (range 5))
               (add-edge-> (node inc :id :a))
	       (add-edge (terminal-node dec :id :d))
	       (add-edge (terminal-node inc :id :final))
               run-graph
	       terminal-queues))
  out
)
	 
    


