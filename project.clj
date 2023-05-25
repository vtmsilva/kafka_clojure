(defproject treinamento "0.1.0-SNAPSHOT"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[com.fzakaria/slf4j-timbre "0.3.17"] 
                 [org.apache.kafka/kafka-streams "3.2.1"]
                 [org.clojure/clojure "1.10.0"]
                 [org.clojure/data.json "2.4.0"]]
  :main ^:skip-aot treinamento.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
