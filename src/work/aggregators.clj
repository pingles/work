(ns work.aggregators
  (:import java.util.concurrent.Executors)
  (:use [plumbing.core]
	[clojure.contrib.map-utils :only [deep-merge-with]]
	[store.api :only [hashmap-bucket bucket-merge-to! bucket-close
			  bucket-put bucket-update bucket-sync bucket-seq
			  with-merge default-bucket-merge]]
	[work.core :only [available-processors seq-work
			  map-work schedule-work shutdown-now]]
	[work.queue :only [local-queue]]))

(defprotocol IAgg
  (agg [this v][this k v])
  (agg-inc [this][this v]))

(defn with-flush [bucket merge-fn flush? secs]
  (let [mem-bucket (java.util.concurrent.atomic.AtomicReference. (hashmap-bucket))		
	do-flush! #(let [cur (.getAndSet mem-bucket (hashmap-bucket))]
		     (bucket-merge-to! cur
		       (with-merge bucket merge-fn)))
	pool (schedule-work
	      #(when (flush?)
		 (do-flush!))
	      secs)]
    [(reify store.api.IWriteBucket
	    (bucket-merge [this k v]
			   (default-bucket-merge
			     (.get mem-bucket) merge-fn
			     k v))
     (bucket-sync [this]
		  (do-flush!)
		  (silent bucket-sync bucket))
     (bucket-close [this] (bucket-close bucket)))
    pool]))

(defn +maps [ms]
  (apply
   deep-merge-with
   + (remove nil? ms)))

(defn agg-bucket [bucket merge done]
  (let [counter (java.util.concurrent.atomic.AtomicInteger.)
	do-and-check (fn [f]
		       (try (f)
			    (finally
			     (when
				 (<= (.decrementAndGet counter) 0)
 			       (done bucket)))))]
    (reify IAgg
	     (agg [this k v]
			    (do-and-check
			     #(bucket-update
			       bucket k (fn [x] (merge [x v])))))
	     (agg [this v]
			    (do-and-check
			     #(bucket-merge-to!
			       v
			       (with-merge bucket
				 (fn [k & args] (merge args))))))
	     (agg-inc [this v] (.addAndGet counter v))
	     (agg-inc [this] (.incrementAndGet counter)))))

(defn mem-agg [merge]
  (agg-bucket (hashmap-bucket) merge
	      #(into {} (bucket-seq %))))

(defn agg-work
  [f num-threads aggregator tasks]
  (let [tasks (seq tasks)
	pool (Executors/newFixedThreadPool num-threads)]
    (agg-inc aggregator (int (count tasks)))
    (doseq [t tasks :let [work #(f t)]]	       
      (.submit pool ^java.lang.Runnable work))
    (shutdown-now pool)))