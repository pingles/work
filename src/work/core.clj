(ns work.core
  (:refer-clojure :exclude [peek])
  (:require [clj-json [core :as json]]
            [clojure.contrib.logging :as log])
  (:use work.queue
	work.message
	clj-serializer.core
	[clojure.contrib.def :only [defvar]]
	[plumbing.core :only [print-all with-ex with-log]])
  (:import (java.util.concurrent
            Executors ExecutorService TimeUnit
            LinkedBlockingQueue)
           clojure.lang.RT))

(defn available-processors []
  (.availableProcessors (Runtime/getRuntime)))

(defn schedule-work
  "schedules work. cron for clojure fns. Schedule a single fn with a pool to run every n seconds,
  where n is specified by the rate arg, or supply a vector of fn-rate tuples to schedule a bunch of fns at once."
  ([^ExecutorService pool f rate]
     (.scheduleAtFixedRate
      pool (with-log f) (long 0) (long rate) TimeUnit/SECONDS)
     pool)
  ([jobs]
     (let [pool (Executors/newSingleThreadScheduledExecutor)] 
       (doall (for [[f rate] jobs]
                (schedule-work pool f rate)))
       pool)))

(defn- work*
  [fns threads]
  (let [pool (Executors/newFixedThreadPool threads)]
    (.invokeAll pool ^java.util.Collection fns)))

(defn work
  "takes a seq of fns executes them in parallel on n threads, blocking until all work is done."
  [fns threads]
  (map #(.get %) (work* fns threads)))

(defn map-work
  "like clojure's map or pmap, but takes a number of threads, executes eagerly, and blocks.

   CHANGE 11/26/2010: num-threads arguments 2nd rather than last arg"
  [f num-threads xs]  
  (if (seq? num-threads)    
    (do (log/warn "map-work arguments have changed, now num-threads is 2nd argument. xs comes last")	
	(recur f xs num-threads))
    (work (doall (map (fn [x] #(f x)) xs)) num-threads)))

(defn filter-work
  "use work to perform a filter operation"
  [f num-threads xs]
  (if (seq? num-threads)
    (do (log/warn "filter-work arguments have changed, now num-threads is 2nd argument. xs comes last")	
	(recur f xs num-threads))
    (filter identity
	    (map-work
	     (fn [x] (if (f x) x nil))	       
	     num-threads
	     xs))))

(defn do-work
  "like clojure's dorun, for side effects only, but takes a number of threads."
  [^java.lang.Runnable f num-threads xs]
  (if (seq? num-threads)
    (do (log/warn "do-work arguments have changed, now num-threads is 2nd argument. xs comes last")	
	(recur f xs num-threads))
    (let [pool (Executors/newFixedThreadPool num-threads)
	  _ (doall (map
		    (fn [x]
		      (let [^java.lang.Runnable fx #(f x)]
			(.submit pool fx)))
		    xs))]
      pool)))

(defn mk-async-task [f args put-done]
  {:f f
   :args args
   :put-done put-done})

(defn async-task-worker
  ([deflt-put-done]
     (fn [{:as task
	   :keys [f,args,put-done]
	   :or {deflt-put-done put-done}} _]
       (let [res (apply f args)]
	 (when put-done (put-done res))
	 res)))
  ([] (async-task-worker nil)))

;;TODO; unable to shutdown pool. seems recursive fns are not responding to interrupt. http://download.oracle.com/javase/tutorial/essential/concurrency/interrupt.html
;;TODO: use another thread to check futures and make sure workers don't fail, don't hang, and call for work within their time limit?
(defn queue-work
  "schedule-work one worker function f per thread.
  f can either be a fn that is directly applied to each task (all workers use the same fn) or
  f builds and evals a worker from a fn & args passed over the queue (each worker executes an arbitrary fn.)
  Examples of the latter are clj-worker and json-wroker.

  Each worker fn polls the work queue via get-work fn, applies a fn to each dequeued item, puts the result with
  put-done and recursively checks for more work.  If it doesn't find new work, it waits until checking for more work.

  The workers can run in asynchronous mode, where the put-done function is passed to the worker function f,
  and f is responsible for ensuring that put-done is appropriately called.
  Valid values for mode are :sync or :async.  If a mode is not specified, queue-work defaults to :sync.

  All error and fault tolernace should be done by client using plumbing.core."
  ([f get-work put-done threads]
     (queue-work f get-work put-done threads :sync))
  ([f get-work put-done threads mode]
     (let [put-done (if (fn? put-done)
                      put-done
                      (fn [k & args]
                        (apply (put-done k) args)))
           pool (Executors/newFixedThreadPool threads)
           fns (repeat threads
                       (fn [] (do
                                (if-let [task (get-work)]
				  (if (= :async mode)
                                      (f task put-done)
                                      (put-done (f task)))
                                  (Thread/sleep 5000))
                                (recur))))
           futures (doall (map #(.submit pool %) fns))]
       pool)))

(defn queue-async-work
  "queue asynchronous work where each task from
   get-work is assumed to come from mk-async-task
   which can have a task-specific put-done function."
  [get-work num-threads &
   {:keys [default-put-done]}]
  (queue-work
    (async-task-worker default-put-done)
    get-work
    nil
    num-threads
    :async))

(defn async-pipeline
  "each pipe-specs consists of the following
     f: function (required)
  "
  [input-work pipe-specs output-work])

(defn shutdown
  "Initiates an orderly shutdown in which previously submitted tasks are executed, but no new tasks will be accepted. Invocation has no additional effect if already shut down."
  [executor]
  (do (.shutdown executor) executor))

(defn shutdown-now [executor]
  "Attempts to stop all actively executing tasks, halts the processing of waiting tasks, and returns a list of the tasks that were awaiting execution.

  There are no guarantees beyond best-effort attempts to stop processing actively executing tasks. For example, typical implementations will cancel via Thread.interrupt(), so if any tasks mask or fail  to respond to interrupts, they may never terminate."
  (do (.shutdownNow executor) executor))

(defn two-phase-shutdown
  "Shuts down an ExecutorService in two phases.
  Call shutdown to reject incoming tasks.
  Calling shutdownNow, if necessary, to cancel any lingering tasks.
  From: http://download-llnw.oracle.com/javase/6/docs/api/java/util/concurrent/ExecutorService.html"
  [^ExecutorService pool]
  (do (.shutdown pool)  ;; Disable new tasks from being submitted
      (with-ex ;; Wait a while for existing tasks to terminate
	(fn [e _ _]
	  (when (instance? InterruptedException e)
	    ;;(Re-)Cancel if current thread also interrupted
	    (.shutdownNow pool)
	    ;; Preserve interrupt status
	    (.interrupt (Thread/currentThread))))	
        #(if (not (.awaitTermination pool 60 TimeUnit/SECONDS))
	  (.shutdownNow pool) ; // Cancel currently executing tasks
          ;;wait a while for tasks to respond to being cancelled
          (if (not (.awaitTermination pool 60 TimeUnit/SECONDS))
            (println "Pool did not terminate" *err*))))))
