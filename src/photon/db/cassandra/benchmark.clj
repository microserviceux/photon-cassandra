(ns photon.db.cassandra.benchmark
  (:gen-class)
  (:require [photon.db.cassandra :refer :all]
            [clojure.math.combinatorics :as combo]
            [photon.db :refer :all]))

(defn random-map [n]
  (let [r (int (* (Math/random) n))]
    (zipmap (take r (repeatedly #(.toString (java.util.UUID/randomUUID))))
            (take r (repeatedly #(random-map (dec n)))))))

(defn send-events! [db n type enc]
  (println "Generating payloads...")
  (let [random-maps
        (doall (take n (repeatedly #(random-map
                                      ({:simple 0
                                        :easy 2
                                        :medium 4
                                        :hard 6
                                        :complex 8}
                                       type)))))]
    (println "Starting writes")
    (dorun
      (pmap #(let [m {:server-timestamp (System/currentTimeMillis)
                      :order-id (System/nanoTime)
                      :stream-name "test"
                      :payload %}]
               (binding [clj-encode (get {:json clj-encode-json
                                          :edn clj-encode-edn
                                          :stream clj-encode-json-stream
                                          :smile clj-encode-smile}
                                     enc)]
                 (store db m)))
            random-maps))))

(defn read-all! [db enc]
  (println "Reading all...")
  (binding [clj-decode (get {:json clj-decode-json
                             :stream clj-decode-json-stream
                             :edn clj-decode-edn
                             :smile clj-decode-smile}
                                     enc)]
    (dorun (map identity (lazy-events db :__all__ 0)))))

(defn warmup! [db]
  (println "Warming up JIT...")
  (delete-all! db)
  (send-events! db 10000 :simple :stream)
  (send-events! db 10000 :simple :edn)
  (send-events! db 10000 :simple :json)
  (send-events! db 10000 :simple :smile))

(defn test-writes! [db type enc num]
  (delete-all! db)
  (let [start (System/nanoTime)]
    (println "Testing...")
    (send-events! db num type enc)
    (let [end (System/nanoTime)
          t-writes (double (/ num (double (/ (double (- end start)) 1000000000.0))))]
      (println "Test for writes" num "|" type "|" enc "| events/s:" t-writes)
      (Thread/sleep 10000)
      (let [rstart (System/nanoTime)]
        (read-all! db enc)
        (let [rend (System/nanoTime)
              t-reads (double (/ num (double (/ (double (- rend rstart)) 1000000000.0))))]
          (println "Test for reads" num "|" type "|" enc
                   "| events/s:" t-reads)
          [{:mode :reads :type type :enc enc :num num :t t-reads}
           {:mode :writes :type type :enc enc :num num :t t-writes}])))))

(defn -main [& args]
  (let [db (->DBCassandra)]
    (warmup! db)
    (let [nums [1 5 10 50 100 500 1000 5000 10000 50000 100000 500000]
          complexities [:simple :easy :medium :hard :complex]
          codecs [:stream :edn :json :smile]
          combs (combo/cartesian-product complexities codecs nums)
          results (mapcat #(apply test-writes! db %) combs)]
      (spit "/tmp/results.benchmark" "" :append false)
      (dorun (map #(spit "/tmp/results.benchmark" (str (pr-str %) "\n")
                         :append true)
                  results)))))

