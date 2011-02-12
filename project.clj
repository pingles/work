(defproject work "0.2.3-SNAPSHOT"
  :description "Clojure workers."
  :url "http://github.com/clj-sys/work"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [clj-serializer "0.1.1"]
                 [clj-sys/plumbing "0.1.3-SNAPSHOT"]
                 [clj-time "0.2.0-SNAPSHOT"]
                 [store "0.1.8-SNAPSHOT"]
                 [woven/clj-json "0.3.1"]]
  :dev-dependencies [[swank-clojure "1.3.0-SNAPSHOT"]
                     [lein-clojars "0.5.0"]
                     [lein-run "1.0.0"]
                     [robert/hooke "1.1.0"]]
  :test-selectors {:default (fn [v] (not (or (:integration v)
                                             (:system v))))
                   :integration :integration
                   :system :system
                   :independent :independent
                   :all (fn [_] true)}
  :repositories {"snapshots" "http://mvn.getwoven.com/repos/woven-public-snapshots"
                 "releases" "http://mvn.getwoven.com/repos/woven-public-releases"})
