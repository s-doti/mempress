(defproject com.github.s-doti/mempress "1.0.1"
  :description "In-memory objects compression"
  :url "https://github.com/s-doti/mempress"
  :license {:name "Apache License, Version 2.0"
            :url  "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/core.memoize "1.0.257"]
                 [clj-time "0.15.2"]
                 [com.taoensso/nippy "3.2.0"]
                 [com.taoensso/timbre "6.2.1"]
                 [com.taoensso/encore "3.60.0"]]
  :profiles {:dev {:dependencies [[midje "1.10.9"]]}}
  :repl-options {:init-ns mempress.core})
