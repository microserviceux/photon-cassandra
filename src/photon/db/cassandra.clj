(ns photon.db.cassandra
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [photon.db :as db]
            [photon.config :as conf]
            [clojure.set :refer [rename-keys]]
            [clojurewerkz.cassaforte.client :as cc]
            [clojure.java.io :as io]
            [clojurewerkz.cassaforte.cql :as cql]
            [taoensso.nippy :as nippy]
            [dire.core :refer [with-handler!]])
  (:use clojurewerkz.cassaforte.query)
  (:import (java.nio.charset Charset CharsetEncoder CharsetDecoder)
           (java.nio CharBuffer Buffer ByteBuffer)
           (java.io PushbackReader)
           (java.util Arrays)
           (java.math BigInteger)))

(def chunk-size 100)

;; This is what happens when things are NOT thread-safe -.-
(def charset (Charset/forName "UTF-8"))
(def encoders (ref {}))
(def decoders (ref {}))
(defn new-encoder [^Charset charset] (.newEncoder charset))
(defn new-decoder [^Charset charset] (.newDecoder charset))
(defn m-encoder [^Thread t]
  (if (contains? @encoders t)
    (get @encoders t)
    (let [e (new-encoder charset) ]
      (dosync (alter encoders assoc t e))
      e)))
(defn m-decoder [^Thread t]
  (if (contains? @decoders t)
    (get @decoders t)
    (let [e (new-decoder charset)]
      (dosync (alter decoders assoc t e))
      e)))
(def encoder (memoize m-encoder))
(def decoder (memoize m-decoder))

(def cassandra-instances (ref {}))
(def conn-string (get conf/config :cassandra.ip "127.0.0.1"))
(def kspace (get conf/config :kspace "photon"))
(def table (get conf/config :table "events"))

(def schema
  {:service_id       :varchar
   :local_id         :varchar
   :provenance       (map-type :varchar :varchar)
   :encoding         :varchar
   :server_timestamp :bigint
   :schema_url       :varchar
   :stream_name      :varchar
   :photon_timestamp :bigint
   :order_id         :bigint
   :payload          :blob
   :primary-key      [:stream_name :order_id]})

(def schema-times
  {:server_timestamp :bigint
   :stream_name      :varchar
   :order_id         :bigint
   :primary-key      [:stream_name :server_timestamp :order_id]})

(defn init-table [conn table]
  (cql/create-table conn table (column-definitions schema))
  (cql/create-table conn (keyword (str (name table) "_times"))
                    (column-definitions schema-times))
  (cql/create-index conn table :service_id)
  (cql/create-index conn table :local_id))

(defn connection [ip]
  (let [ref-conn (get @cassandra-instances ip)]
    (if (nil? ref-conn)
      (do
        (let [conn (cc/connect [ip])]
          (dosync (alter cassandra-instances assoc ip conn))
          (log/trace (pr-str @cassandra-instances))
          (cql/use-keyspace conn kspace)
          (init-table conn table)
          conn))
      ref-conn)))

(defn position [^Buffer b] (.position b))
(defn set-position! [^Buffer b ^Integer i] (.position b i))
(defn decode [^CharsetDecoder d ^Buffer b]
  (.decode d b))
(defn buffer->string [^CharBuffer cb] (.toString cb))

(defn encode [^CharsetEncoder e ^String s]
  (.encode e (CharBuffer/wrap s)))

(defn long-value [^BigInteger bi] (.longValue bi))

(defn byte-output-stream [v i bb]
  (proxy [java.io.OutputStream] []
    (close []
      (let [new-bb (ByteBuffer/allocateDirect @i)]
        (dorun (map #(.put new-bb (nth % 0) (nth % 1) (nth % 2))
                    (persistent! @v)))
        (.flip new-bb)
        (reset! bb new-bb)))
    (write [#^bytes b ^Integer off ^Integer len]
      (let [ve @v]
        (reset! v (conj! ve [(Arrays/copyOf b len) off len])))
      (swap! i + len))))

(defn byte-input-stream [^ByteBuffer bb]
  (proxy [java.io.InputStream] []
    (available [] (.remaining bb))
    (read
      ([] (if (.hasRemaining bb) (.get bb) -1))
      ([#^bytes b ^Integer off ^Integer len]
       (let [c (min (.remaining bb) len)]
         (if (= 0 c)
           -1
           (do (.get bb b off c) c)))))))

(defn clj-encode-edn-stream [item]
  (let [bb (atom nil)
        the-boss (byte-output-stream (atom (transient [])) (atom 0) bb)]
    (with-open [w (io/writer the-boss)]
      (binding [*out* w]
        (pr item)))
    @bb))

(defn clj-decode-edn-stream [data]
  (with-open [r (PushbackReader. (io/reader (byte-input-stream data)) 8192)]
    (read r)))

(defn clj-encode-json-stream [item]
  (let [bb (atom nil)
        the-boss (byte-output-stream (atom (transient [])) (atom 0) bb)]
    (with-open [w (io/writer the-boss)]
      (json/generate-stream item w))
    @bb))

(defn clj-decode-json-stream [data]
  (with-open [r (io/reader (byte-input-stream data))]
    (json/parse-stream r true)))

(defn clj-encode-nippy [item]
  (ByteBuffer/wrap (nippy/freeze item)))
(defn clj-decode-nippy [data]
  (nippy/thaw (.array data)))

(defn clj-encode-edn [item]
  (encode (encoder (Thread/currentThread))
          (pr-str item)))
(defn clj-decode-edn [data]
  (let [d (decoder (Thread/currentThread))
        item (decode d data)]
    (read-string (buffer->string item))))

(defn clj-encode-json [item]
  (encode (encoder (Thread/currentThread))
          (json/generate-string item)))
(defn clj-decode-json [data]
  (let [d (decoder (Thread/currentThread))
        item (decode d data)]
    (json/parse-string (buffer->string item) true)))

(defn remaining [^Buffer b] (.remaining b))
(defn get-from-buffer [^ByteBuffer b #^bytes bb] (.get b bb))
(def clj-encode-smile json/generate-smile)
(defn clj-decode-smile [data]
  (let [bb (byte-array (remaining data))]
    (get-from-buffer data bb)
    (json/parse-smile bb true)))

(def ^:dynamic clj-encode json/generate-string)
(def ^:dynamic clj-decode json/parse-string)

(defn clj->cassandra [cl]
  (let [rm (rename-keys cl {:stream-name :stream_name
                            :service-id :service_id
                            :order-id :order_id
                            :server-timestamp :server_timestamp
                            :photon-timestamp :photon_timestamp
                            :local-id :local_id
                            :schema :schema_url})]
    (assoc
     (assoc rm :payload (clj-encode (:payload cl)))
     :server_timestamp (long-value (biginteger (:server_timestamp rm))))))

(defn cassandra->clj [cass]
  (log/trace (pr-str cass))
  (let [rm (rename-keys cass {:stream_name :stream-name
                              :service_id :service-id
                              :photon_timestamp :photon-timestamp
                              :order_id :order-id
                              :server_timestamp :server-timestamp
                              :local_id :local-id
                              :schema_url :schema})
        p (:payload cass)]
    (if (nil? p)
      {}
      (let [old-position (position p)
            res (assoc rm :payload (clj-decode p))]
        (set-position! p old-position)
        (log/trace (pr-str res))
        res))))

(defn ordered-combination
  [sort-fn seqs]
  (if (every? empty? seqs)
    '()
    (let [sorted (sort-by #(sort-fn (first %)) seqs)
          chosen (first sorted)
          new-seqs (cons (rest chosen) (rest sorted))]
      (cons (first chosen)
            (lazy-seq (ordered-combination sort-fn new-seqs))))))

(let [r
      (defrecord DBCassandra []
        db/DB
        (db/driver-name [this] "cassandra")
        (db/fetch [this stream-name id]
          (log/trace "Fetching " stream-name " " id)
          (first
            (map
              cassandra->clj
              (let [conn (connection conn-string)]
                (cql/select conn table (where [[= :stream_name stream-name]
                                               [= :order_id (bigint id)]]))))))
        (db/delete! [this id]
          (let [conn (connection conn-string)]
            (cql/delete conn table (where [[= :uuid id]]))))
        (db/delete-all! [this]
          (let [conn (connection conn-string)]
            (cql/drop-table conn table)
            (cql/drop-table conn (keyword (str (name table) "_times")))
            (cql/drop-keyspace conn kspace)
            (cql/create-keyspace conn kspace
                                 (with {:replication
                                        {:class "SimpleStrategy"
                                         :replication_factor 1}}))
            (init-table conn table)))
        (db/put [this data]
          (db/store this data))
        (db/search [this id]
          (map
            cassandra->clj
            (let [conn (connection conn-string)]
              (cql/select conn table {:uuid id}))))
        (db/store [this payload]
          (let [conn (connection conn-string)]
            (cql/insert conn table (clj->cassandra payload))
            (cql/insert
              conn (keyword (str (name table) "_times"))
              {:server_timestamp (:server-timestamp payload)
               :stream_name (:stream-name payload)
               :order_id (:order-id payload)})))
        (db/distinct-values [this k]
          (let [conn (connection conn-string)
                ck (get {:stream-name :stream_name
                         :service-id :service_id
                         :order-id :order_id
                         :server-timestamp :server_timestamp
                         :photon-timestamp :photon_timestamp
                         :local-id :local_id
                         :schema :schema_url}
                        k k)]
            (map :dv (cql/select conn table
                                 (columns (-> ck distinct* (as "dv")))))))
        (db/lazy-events [this stream-name date]
          (log/info "Retrieving events from" stream-name "from date" date)
          (if (or (= "__all__" stream-name)
                  (= :__all__ stream-name))
            (let [sts (db/distinct-values this :stream-name)]
              (ordered-combination :server-timestamp
                                   (map #(db/lazy-events this % date) sts)))
            (let [conn (connection conn-string)]
              (let [res (cql/select conn (keyword (str (name table) "_times"))
                                    (where [[= :stream_name stream-name]
                                            [>= :server_timestamp date]])
                                    (order-by [:server_timestamp :asc])
                                    (limit 1))
                    first-ts (:order_id (first res))]
                (if (empty? res)
                  []
                  (db/lazy-events-page this stream-name date first-ts))))))
        (db/lazy-events-page [this stream-name date page]
          (let [conn (connection conn-string)]
            (let [res (cql/select conn table
                                  (where [[= :stream_name stream-name]
                                          [>= :order_id page]])
                                  (order-by [:order_id :asc])
                                  (limit chunk-size))]
              (if (empty? res)
                []
                (let [last-ts (inc (:order_id (last res)))]
                  (concat (map cassandra->clj res)
                          (lazy-seq (db/lazy-events-page
                                      this stream-name date last-ts)))))))))]
  (dosync (alter db/set-records conj (db/class->record r))))

(defn create-keyspace [c]
  (let [conn (cc/connect ["127.0.0.1"])]
    (cql/create-keyspace conn "cassaforte_keyspace"
                         (with {:replication
                                {:class "SimpleStrategy"
                                 :replication_factor 1}}))))

(defn drop-keyspace []
  (let [conn (cc/connect ["127.0.0.1"])]
    (cql/drop-keyspace conn "cassaforte_keyspace")))

(defn test-cassandra []
  (drop-keyspace)
  (create-keyspace)
  (let [conn (cc/connect ["127.0.0.1"])]
    (cql/use-keyspace conn "cassaforte_keyspace")
    (log/trace "Creating table...")
    (cql/create-table conn "users"
                      (column-definitions {:name :varchar
                                           :age  :int
                                           :data (map-type :varchar
                                                           :blob)
                                           :primary-key [:name]}))
    (dorun (take 10
                 (repeatedly
                  #(cql/insert
                    conn "users"
                    {:name (.toString (java.util.UUID/randomUUID))
                     :age (int (rand-int 100))}))))
    (log/trace "Done storing...")
    (cql/iterate-table conn "users" :name 100)))

(defn exception->message [^Throwable t] (.getMessage t))

(with-handler! #'cql/use-keyspace
  com.datastax.driver.core.exceptions.InvalidQueryException
  (fn [_ conn k & args]
    (cql/create-keyspace conn k
                         (with {:replication
                                {:class "SimpleStrategy"
                                 :replication_factor 1}}))
    (cql/use-keyspace conn k)))

(with-handler! #'cql/drop-keyspace
  com.datastax.driver.core.exceptions.InvalidQueryException
  (fn [e & args]
    (log/trace "Dropping non-existing keyspace")))

(with-handler! #'cql/create-table
  com.datastax.driver.core.exceptions.AlreadyExistsException
  (fn [e & args]
    (log/trace "Already exists")))

(with-handler! #'cql/drop-table
  com.datastax.driver.core.exceptions.InvalidQueryException
  (fn [e & args]
    (log/trace "Dropping non-existing table")))

(with-handler! #'cql/create-keyspace
  com.datastax.driver.core.exceptions.AlreadyExistsException
  (fn [e & args]
    (log/trace "Already exists")))

(with-handler! #'cql/create-index
  com.datastax.driver.core.exceptions.AlreadyExistsException
  (fn [e & args]
    (log/trace "Already exists")))

(with-handler! #'cql/create-index
  com.datastax.driver.core.exceptions.InvalidQueryException
  (fn [e & args]
    (log/trace "Already exists")))

