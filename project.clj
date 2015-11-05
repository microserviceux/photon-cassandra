(defproject tranchis/photon-cassandra "0.9.40"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
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
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [tranchis/photon-db "0.9.31"]
                 [tranchis/cassaforte "2.1.0-beta3"]
                 [org.clojure/math.combinatorics "0.1.1"]
                 [cheshire "5.5.0"]
                 [pjson "0.2.9"]
                 [org.clojure/tools.reader "0.10.0-alpha3"]
                 [com.taoensso/nippy "2.10.0"
                  :exclusions [org.clojure/tools.reader]]
                 [dire "0.5.3"]
                 [org.clojure/tools.logging "0.3.1"]])

