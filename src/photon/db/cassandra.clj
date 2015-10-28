(ns photon.db.cassandra
  (:require [clojure.tools.logging :as log]
            [photon.db.cassandra.encoding :as enc]
            [photon.db :as db]
            [clojure.set :refer [rename-keys]]
            [clojurewerkz.cassaforte.client :as cc]
            [clojurewerkz.cassaforte.cql :as cql]
            [dire.core :refer [with-handler!]])
  (:use clojurewerkz.cassaforte.query)
  (:import (java.nio Buffer)
           (java.math BigInteger)))

(def chunk-size 100)

(defn long-value [^BigInteger bi] (.longValue bi))

(def cassandra-instances (ref {}))
(defn conn-string [conf]
  (get conf :cassandra.ip "127.0.0.1"))
(defn kspace [conf]
  (get conf :kspace "photon"))
(defn table [conf]
  (get conf :table "events"))

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

(defn connection [conf]
  (let [ip (conn-string conf)
        ref-conn (get @cassandra-instances ip)]
    (if (nil? ref-conn)
      (do
        (let [conn (cc/connect [ip])]
          (dosync (alter cassandra-instances assoc ip conn))
          (log/trace (pr-str @cassandra-instances))
          (cql/use-keyspace conn (kspace conf))
          (init-table conn (table conf))
          conn))
      ref-conn)))

(def ^:dynamic clj-encode enc/clj-encode-nippy)
(def ^:dynamic clj-decode enc/clj-decode-nippy)

(defn position [^Buffer b] (.position b))
(defn set-position! [^Buffer b ^Integer i] (.position b i))

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
      (defrecord DBCassandra [conf]
        db/DB
        (db/driver-name [this] "cassandra")
        (db/fetch [this stream-name id]
          (log/trace "Fetching " stream-name " " id)
          (first
            (map
              cassandra->clj
              (let [conn (connection conf)]
                (cql/select conn (table conf)
                            (where [[= :stream_name stream-name]
                                    [= :order_id (bigint id)]]))))))
        (db/delete! [this id]
          (let [conn (connection conf)]
            (cql/delete conn (table conf)
                        (where [[= :uuid id]]))))
        (db/delete-all! [this]
          (let [conn (connection conf)]
            (cql/drop-table conn (table conf))
            (cql/drop-table conn (keyword (str (name (table conf)) "_times")))
            (cql/drop-keyspace conn (kspace conf))
            (cql/create-keyspace conn kspace
                                 (with {:replication
                                        {:class "SimpleStrategy"
                                         :replication_factor 1}}))
            (init-table conn (table conf))))
        (db/put [this data]
          (db/store this data))
        (db/search [this id]
          (map
            cassandra->clj
            (let [conn (connection conf)]
              (cql/select conn (table conf) {:uuid id}))))
        (db/store [this payload]
          (let [conn (connection conf)]
            (cql/insert conn (table conf) (clj->cassandra payload))
            (cql/insert
              conn (keyword (str (name (table conf)) "_times"))
              {:server_timestamp (:server-timestamp payload)
               :stream_name (:stream-name payload)
               :order_id (:order-id payload)})))
        (db/distinct-values [this k]
          (let [conn (connection conf)
                ck (get {:stream-name :stream_name
                         :service-id :service_id
                         :order-id :order_id
                         :server-timestamp :server_timestamp
                         :photon-timestamp :photon_timestamp
                         :local-id :local_id
                         :schema :schema_url}
                        k k)]
            (map :dv (cql/select conn (table conf)
                                 (columns (-> ck distinct* (as "dv")))))))
        (db/lazy-events [this stream-name date]
          (log/info "Retrieving events from" stream-name "from date" date)
          (if (or (= "__all__" stream-name)
                  (= :__all__ stream-name))
            (let [sts (db/distinct-values this :stream-name)]
              (ordered-combination :server-timestamp
                                   (map #(db/lazy-events this % date) sts)))
            (let [conn (connection conf)]
              (let [res (cql/select conn (keyword (str (name (table conf)) "_times"))
                                    (where [[= :stream_name stream-name]
                                            [>= :server_timestamp date]])
                                    (order-by [:server_timestamp :asc])
                                    (limit 1))
                    first-ts (:order_id (first res))]
                (if (empty? res)
                  []
                  (db/lazy-events-page this stream-name date first-ts))))))
        (db/lazy-events-page [this stream-name date page]
          (let [conn (connection conf)]
            (let [res (cql/select conn (table conf)
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

