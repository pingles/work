(ns work.graph
  (:require
   [clojure.contrib.logging :as log]
   [clojure.zip :as zip]
   [work.core :as work]
   [clojure.contrib.zip-filter :as zf]
   [work.queue :as workq]))

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
  (broadcast [this x]
    "broadcast output x to neighbors or for side-effect")
  (add-listener [this listener]
     "return new Outbox with another listener.
      OPTIONAL."))

(defprotocol VertexProcess 
  (start-process [this f inbox outbox]
    "process information from inbox to outbox with fn f"))

;;  New Graph Stuff

(defrecord Vertex [f inbox outbox process])

(defn drain-to-vertex
  "send seq to vertex inbox, returns vertex"
  [vertex xs]
  (doseq [x xs] (receive-message (:inbox vertex) :outside x))
  vertex)

;; Inbox implementations

(defrecord InboxQueue [q]
  Inbox
  (receive-message [this src msg] (workq/offer q msg))
  (poll-message [this] (workq/poll q)))

;; Broadcast Policy

; outs is seq of Vertexs
(defrecord BroadcastOutbox [outs]
  Outbox
  (broadcast [this x]
             (doseq [o outs]
               (receive-message (:inbox o) this x)))
  (add-listener [this listener] (update-in this [:outs] conj listener)))

;; For a terminal
(defrecord TerminalOutbox [out-queue]
  Outbox
  (broadcast [this x] (when out-queue (workq/offer out-queue x))))

;; (defrecord DispatchOutbox [dispatches graph]
;;   Outbox
;;   (broadcast [this x]
;;       (doseq [disp dispatches
;;               out-id (disp x)
;;               out (graph out-id)
;;               :when out]
;;         (receive-message (:inbox out) this x)))
;;   (add-listener [this dispatch-fn]
;;          (update-in this [:dispatches] conj dispatch-fn)))


;; Queue-Work

(defrecord QueueWorkProcess [threads sleep-time exec]
  VertexProcess
  (start-process [this f inbox outbox]
   (future (work/queue-work
            (into {}
              (merge this
                {:f f
                 :in (partial poll-message inbox)
                 :out (partial broadcast outbox)}))))))

;; Vertex process policy for an "input" vertex  where we inject data
(defrecord InputProcess [freq]
  VertexProcess
  (start-process [this f _ outbox]
      (future (work/schedule-work
               freq
               #(broadcast outbox (f))))))

(defn mk-process
  "process factory; pass in options"
  [kw &
   {:keys [threads sleep-time freq exec]
    :or {threads (work/available-processors)
         sleep-time 5000
         freq 200
         exec work/sync}}]
  (case kw
    :queue-work (QueueWorkProcess. threads sleep-time exec)
    :input (InputProcess. freq)))

;; Start Vertex process put results in :process of vertex

(defn- run-vertex
  "launch vertex return vertex with :pool field"
  [{:keys [f,process,inbox,outbox] :as vertex}]
  (assoc vertex
      :process (start-process process f inbox outbox)))

(defn- mk-node
  "make a node. only required argument is the fn f at the vertex.
   you can optionally pass in inbox, process, id, or outbox

   Defaults
   --------
   id: gensymd
   process: queue-work with defaults
   inbox: InboxQueue with new empty queue
   outbox: BroadcastOutbox no neighbors"
  [f  &
   {:keys [id inbox outbox process]
    :or {inbox (InboxQueue. (workq/local-queue))
         outbox (BroadcastOutbox. [])
         process :queue-work
         id (gensym)}
    :as opts}]
  (-> f
      (Vertex.  inbox outbox (apply mk-process process
                                    (apply concat (seq opts))))
      (assoc :id id)))

(defn broadcast-node
  "make a new broadcast-node with no neighbors"
  [f & opts]
  (apply mk-node f
         :outbox (BroadcastOutbox. [])
         opts))

;; (defn dispatch-node [f disp & opts]
;;   (apply mk-node f :outbox (DispatchOutbox. [disp] nil) opts))

(defn terminal-node
  "make a terminal outbox node. same arguments as mk-node
   except that you can pass an :out argument for the queue
   you want the vertex to drain to"
  [f & {:keys [out] :as opts}]
  (apply mk-node f
         :outbox (if out (TerminalOutbox. out) (TerminalOutbox. (workq/local-queue)))
         (apply concat (seq opts))))

(defn- root-node
  "make the root node. same as mk-node except :input-data argument
   lets you pass data to the node's inbox directly"
  [& {:keys [input-data, freq] :or {freq 500} :as opts}]
  (apply mk-node identity
           :id :root
           :inbox (InboxQueue. (if input-data
                     (workq/local-queue input-data)
                     (workq/local-queue)))
           (apply concat  (seq opts))))

(defn- add-edge-internal
  [src trg]
  (update-in src [:outbox :outs] conj trg))

(defn- graph-zip
  "make a zipper out of a graph"
  [root]
  (zip/zipper
   ; branch?
   (fn [{:keys [outbox]}] (instance? BroadcastOutbox outbox))
   ; children
   (comp :outs :outbox)
   ; make-node
   (fn [n cs]
     (assoc n :outbox (BroadcastOutbox. (vec cs))))
   ; root 
   root))

(defn new-graph
  "construct a new graph (just the root vertex). Use :input-data
   optional arg pass in data. Root vertex gets :id of :root.

   Returns graph zipper lcoation"
  [& {:keys [input-data, freq] :or {freq 500} :as opts}]
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
    root
    ))

(defn terminal-queues [root]
  (reduce
   (fn [res v]
     (assoc res (:id v) (-> v :outbox :out-queue)))
   {}
   (filter #(instance? TerminalOutbox (:outbox %))
	   (all-vertices root))))

(comment
  (def out (-> (new-graph :input-data (range 5))
               (add-edge-> (broadcast-node inc :id :a))
	       (add-edge (terminal-node dec :id :d))
	       (add-edge (terminal-node inc :id :final))
               run-graph
	       terminal-queues))
)
	 
    


