(ns photon.db.cassandra.benchmark
  (:gen-class)
  (:require [photon.db.cassandra :refer :all]
            [clojure.math.combinatorics :as combo]
            [photon.db.cassandra.encoding :refer :all]
            [photon.db :refer :all]))

(defn random-map [n]
  (zipmap (take n (repeatedly #(.toString (java.util.UUID/randomUUID))))
          (take n (repeatedly #(random-map (dec n))))))

(defn send-events! [db n type enc]
  (println "Generating payloads...")
  (let [rm (random-map ({:simple 0 :easy 1 :medium 2 :hard 4 :complex 6}
              type))]
    (println "Starting writes")
    (dorun
      (pmap (fn [_]
              (let [m {:server-timestamp (System/currentTimeMillis)
                       :order-id (System/nanoTime)
                       :stream-name "test"
                       :payload rm}]
                (binding [clj-encode (get {:json clj-encode-json
                                           :edn clj-encode-edn
                                           :edn-stream clj-encode-edn-stream
                                           :nippy clj-encode-nippy
                                           :stream clj-encode-json-stream
                                           :smile clj-encode-smile}
                                          enc)]
                  (store db m))))
            (range n)))))

(defn read-all! [db enc]
  (println "Reading all...")
  (binding [clj-decode (get {:json clj-decode-json
                             :nippy clj-decode-nippy
                             :stream clj-decode-json-stream
                             :edn clj-decode-edn
                             :edn-stream clj-decode-edn-stream
                             :smile clj-decode-smile}
                                     enc)]
    (dorun (map identity (lazy-events db :__all__ 0)))))

(defn warmup! [db]
  (println "Warming up JIT...")
  (delete-all! db)
  (send-events! db 10000 :simple :stream)
  (send-events! db 10000 :simple :nippy)
  (send-events! db 10000 :simple :edn)
  (send-events! db 10000 :simple :edn-stream)
  (send-events! db 10000 :simple :json)
  (send-events! db 10000 :simple :smile))

(defn test-writes! [db num type enc]
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
  (let [db (->DBCassandra {:db.backend "cassandra"})]
    (warmup! db)
    (let [nums [1 5 10 50 100 500 1000 5000 10000 50000 100000 500000]
          complexities [:simple :easy :medium :hard :complex]
          codecs [:nippy :stream :edn :json :smile :edn-stream]
          combs (combo/cartesian-product nums complexities codecs)
          results (mapcat #(apply test-writes! db %) combs)]
      (spit "/tmp/results.benchmark" "" :append false)
      (dorun (map #(spit "/tmp/results.benchmark" (str (pr-str %) "\n")
                         :append true)
                  results)))))

