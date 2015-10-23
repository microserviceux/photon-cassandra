(ns photon.db.cassandra
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [photon.db :as db]
            [photon.config :as conf]
            [clojure.set :refer [rename-keys]]
            [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.cql    :as cql]
            [dire.core :refer [with-handler!]])
  (:use clojurewerkz.cassaforte.query)
  (:import (java.nio.charset Charset CharsetEncoder CharsetDecoder)
           (java.nio CharBuffer Buffer)
           (java.math BigInteger)))

(def chunk-size 100)

(def charset (Charset/forName "UTF-8"))
(def encoder ((fn [^Charset charset] (.newEncoder charset)) charset))
(defn decoder [^Charset charset] (.newDecoder charset))
(def cassandra-instances (ref {}))

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

(defn connection [ip]
  (let [ref-conn (get @cassandra-instances ip)]
    (if (nil? ref-conn)
      (do
        (let [conn (cc/connect [ip])]
          (dosync (alter cassandra-instances assoc ip conn))
          (println (pr-str @cassandra-instances))
          conn))
      ref-conn)))

(defn encode [^CharsetEncoder encoder ^String s]
  (.encode encoder (CharBuffer/wrap s)))

(defn long-value [^BigInteger bi] (.longValue bi))

(defn clj->cassandra [cl]
  (let [rm (rename-keys cl {:stream-name :stream_name
                            :service-id :service_id
                            :order-id :order_id
                            :server-timestamp :server_timestamp
                            :photon-timestamp :photon_timestamp
                            :local-id :local_id
                            :schema :schema_url})]
    (assoc
     (assoc rm :payload
            (encode encoder (json/generate-string (:payload cl))))
     :server_timestamp (long-value (biginteger (:server_timestamp rm))))))

(defn position [^Buffer b] (.position b))
(defn set-position! [^Buffer b ^Integer i] (.position b i))
(defn decode [^CharsetDecoder encoder ^Buffer b]
  (.decode encoder b))
(defn buffer->string [^CharBuffer cb] (.toString cb))

(defn cassandra->clj [cass]
  #_(println (pr-str cass))
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
            decoder (decoder charset)
            data (buffer->string (decode decoder p))
            res (assoc rm :payload (json/parse-string data true))]
        (set-position! p old-position)
        #_(println (pr-str res))
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

(def conn-string (get conf/config :cassandra.ip "127.0.0.1"))
(def kspace (get conf/config :kspace "photon"))
(def table (get conf/config :table "events"))

(db/defdbplugin DBCassandra []
  db/DB
  (db/driver-name [this] "cassandra")
  (db/fetch [this stream-name id]
            (println "Fetching " stream-name " " id)
            (first
             (map
              cassandra->clj
              (let [conn (connection conn-string)]
                (cql/use-keyspace conn kspace)
                (cql/select conn table (where [[= :stream_name stream-name]
                                               [= :order_id (bigint id)]]))))))
  (db/delete! [this id]
              (let [conn (connection conn-string)]
                (cql/use-keyspace conn kspace)
                (cql/delete conn table (where [[= :uuid id]]))))
  (db/delete-all! [this]
                  (let [conn (connection conn-string)]
                    (cql/use-keyspace conn kspace)
                    (cql/drop-table conn table)))
  (db/put [this data]
          (db/store this data))
  (db/search [this id]
             (map
              cassandra->clj
              (let [conn (connection conn-string)]
                (cql/use-keyspace conn kspace)
                (cql/select conn table {:uuid id}))))
  (db/store [this payload]
            (let [conn (connection conn-string)]
              (cql/use-keyspace conn kspace)
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
      (cql/use-keyspace conn kspace)
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
        (cql/use-keyspace conn kspace)
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
      (cql/use-keyspace conn kspace)
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
                               this stream-name date last-ts)))))))))

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
    (println "Creating table...")
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
    (println "Done storing...")
    (cql/iterate-table conn "users" :name 100)))

(defn init-table [conn table]
  (cql/create-table conn table (column-definitions schema))
  (cql/create-table conn (keyword (str (name table) "_times"))
                    (column-definitions schema-times))
  #_(cql/create-index conn table :stream_name)
  (cql/create-index conn table :service_id)
  (cql/create-index conn table :local_id)
  #_(cql/create-index conn table :server_timestamp))

(defn exception->message [^Throwable t] (.getMessage t))

(with-handler! #'cql/insert
  com.datastax.driver.core.exceptions.InvalidQueryException
  (fn [e conn table data & args]
    (let [msg (exception->message e)]
      (println "::insert::" msg "::" (pr-str table) "::" (pr-str data))
      (when-not ((fn [^String s ^String b] (.startsWith s b))
                 msg "Unknown identifier")
        (init-table conn table)
        (cql/insert conn table data)))))

(with-handler! #'cql/select
  com.datastax.driver.core.exceptions.InvalidQueryException
  (fn [e conn table query & args]
    (println "::select::" (exception->message e))
    (init-table conn table)
    []))

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
    (println "Dropping non-existing table")))

(with-handler! #'cql/create-table
  com.datastax.driver.core.exceptions.AlreadyExistsException
  (fn [e & args]
    (println "Already exists")))

(with-handler! #'cql/create-keyspace
  com.datastax.driver.core.exceptions.AlreadyExistsException
  (fn [e & args]
    (println "Already exists")))

(with-handler! #'cql/create-index
  com.datastax.driver.core.exceptions.AlreadyExistsException
  (fn [e & args]
    (println "Already exists")))

(with-handler! #'cql/create-index
  com.datastax.driver.core.exceptions.InvalidQueryException
  (fn [e & args]
    (println "Already exists")))

