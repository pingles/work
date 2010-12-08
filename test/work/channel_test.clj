(ns work.channel-test
  (:use clojure.test work.channel)
  (:require work.queue))

(deftest from-queue-test
  (let [q (work.queue/local-queue [1 2 3 :eof])
	a (atom [])
	[channel start-channel] (from-queue q)
	done? (promise)]
    (add-consumer channel
     (reify ChannelConsumer
	    (event [_ _ elem] (swap! a conj elem))
	    (closed [_ _] (deliver done? nil))))
    (future (start-channel))
    @done?
    (is (= @a [1 2 3]))))

(deftest push-to-queue-test
  (let [[ch start] (from-queue (work.queue/local-queue [1 2 3 :eof]))
	out (work.queue/local-queue)]
    (push-to-queue ch out)
    (start)
    (is (= (seq out) [1 2 3]))))