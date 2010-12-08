(ns work.channel
  (:require work.queue))

(defprotocol ChannelConsumer
  (event [this ch elem] "new element in channel ch event notification")
  (closed [this ch] "notification channel ch is closed"))

(defprotocol ChannelProducer
  (add-consumer [this consumer] "add to consumers to notify"))

(defn push-to-queue
   "takes a channel and pushes its events to makes a queue you can poll  to get elements."    
   [ch q]
   (add-consumer ch
    (reify ChannelConsumer
	   (event [this _ elem] (work.queue/offer q elem))
	   (closed [this ch]))))

(defn from-queue
  "take a queue and return a channel which publishes events polled
   from the queue as well as a fn to execute to begin the polling and channel process.
   The queue needs a sentinal value (default :eof constrolled
   by eof options). Returns ChannelConsumer. "
  [q & {:keys [eof] :or {eof :eof}}]
  (let [listeners (atom nil)	
	producer (reify ChannelProducer
			(add-consumer [_ consumer] (swap! listeners conj consumer)))
	f  #(loop []
	     (let [v (work.queue/poll q)]
	      (if (= v eof)
		(doseq [l @listeners] (closed l producer))
		(do
		  (doseq [l @listeners] (event l producer v))
		  (recur)))))]
    [producer f]))
