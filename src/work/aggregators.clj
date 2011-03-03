(ns work.aggregators
  (:import java.util.concurrent.Executors)
  (:use [plumbing.core]
	[clojure.contrib.map-utils :only [deep-merge-with]]
	store.api
	[work.core :only [available-processors seq-work
			  map-work schedule-work shutdown-now]]
	[work.queue :only [local-queue]]))

(defprotocol IAgg
  (agg [this v][this k v])
  (agg-inc [this][this v]))

(defn with-flush 
  "returns a bucket which accumulated (merges) updates using the merge-fn on an in-memory hashmap-bucket. These
   partial results are merged with the underlying bucket (assumed to implement bucket-merge). If no merge-fn
   is passed in, uses bucket-merger function on underlying bucket."
  ([bucket merge-fn flush? secs]
     (let [mem-bucket (java.util.concurrent.atomic.AtomicReference. (hashmap-bucket))
	   do-flush! #(let [cur (.getAndSet mem-bucket (hashmap-bucket))]
			(bucket-merge-to! cur bucket))
	   pool (schedule-work
		 #(when (flush?)
		    (do-flush!))
		 secs)
	   flush-bucket
	   (reify store.api.IWriteBucket
		  (bucket-merge [this k v]
				(default-bucket-merge
				  (.get mem-bucket) (partial merge-fn k) k v))		      
		  (bucket-sync [this]
			       (do-flush!)
			       (silent bucket-sync bucket))
		  (bucket-close [this] (bucket-close bucket))
		 
		  store.api.IReadBucket
		  (bucket-get [this k]
			      (bucket-get bucket k)))]
       [flush-bucket pool]))
  ([bucket flush? secs] (with-flush bucket (bucket-merger bucket) flush? secs)))

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