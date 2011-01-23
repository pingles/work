(ns work.diagnostics)

(defn to-csv-line [vs]
  (apply str
	 (drop-last
	  (interleave
	   vs
	   (repeat ", ")))))

(defn to-csv [ms]
  (let [ks [:root :feed-fetch :extractor
	    :entry-fetch :complete-entry :final-store-entry]]
    (cons (to-csv-line ks) 
    (map #(to-csv-line
	   (map (fn [k] (k %)) ks))
	 ms))))