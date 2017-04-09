(defproject tranchis/photon-cassandra "0.10.4"
  :description "Cassandra (<= 2.1) plugin for photon"
  :url "https://github.com/microserviceux/photon-cassandra"
  :license {:name "GNU Affero General Public License Version 3"
            :url "https://www.gnu.org/licenses/agpl-3.0.html"}
  #_#_:global-vars {*warn-on-reflection* true}
  :aot :all
  :main photon.db.cassandra.benchmark
  :aliases {"benchmark" ["run" "-m" "photon.db.cassandra.benchmark/-main"]}
  :jvm-opts ["-Xmx4g"]
  #_#_:jvm-opts [#_"-Xmx4g"
             #_"-XX:+PrintGCDetails"
             "-agentpath:/Users/sergio/Downloads/YourKit_Java_Profiler_2015_build_15074.app/Contents/Resources/bin/mac/libyjpagent.jnilib"
             #_"server"
             "-dsa" "-d64" "-da" "-XX:+UseConcMarkSweepGC"
             "-XX:+UseParNewGC" "-XX:ParallelCMSThreads=4"
             "-XX:+ExplicitGCInvokesConcurrent"
             "-XX:+CMSParallelRemarkEnabled"
             "-XX:-CMSIncrementalPacing"
             "-XX:+UseCMSInitiatingOccupancyOnly"
             "-XX:CMSIncrementalDutyCycle=100"
             "-XX:CMSInitiatingOccupancyFraction=90"
             "-XX:CMSIncrementalSafetyFactor=10"
             "-XX:+CMSClassUnloadingEnabled" "-XX:+DoEscapeAnalysis"]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [tranchis/photon-db "0.10.4"]
                 [clojurewerkz/cassaforte "3.0.0-RC1"]
                 [org.clojure/math.combinatorics "0.1.4"]
                 [cheshire "5.7.0"]
                 [pjson "0.3.8"]
                 [org.clojure/tools.reader "0.10.0"]
                 [com.taoensso/nippy "2.13.0"]
                 [dire "0.5.4" :exclusions [slingshot]]
                 [org.clojure/tools.reader "1.0.0-beta4"]
                 [midje "1.8.3"]
                 [org.clojure/tools.logging "0.3.1"]]
  :plugins [[lein-midje "3.2"]])
