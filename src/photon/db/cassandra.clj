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

(defn long-value [^BigInteger bi] (.longValue bi))

(def cassandra-instances (ref {}))
(defn conn-string [conf]
  (get conf :cassandra.ip "127.0.0.1"))
(defn kspace [conf]
  (get conf :kspace "photon"))
(defn table [conf]
  (get conf :table "events"))
(defn chunk-size [conf]
  (get conf :cassandra.buffer 100))

(def schema
  {:order_id           :bigint
   :event_time         :bigint
   :stream_name        :varchar
   :event_type         :varchar
   :caused_by          :varchar
   :caused_by_relation :varchar
   :payload            :blob
   :service_id         :varchar
   :schema_url         :varchar
   :primary-key        [:stream_name :order_id]})

(defn init-table [conn table]
  (cql/create-table conn table (column-definitions schema))
  (cql/create-index conn table :service_id))

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

(def ^:dynamic clj-encode enc/clj-encode-smile)
(def ^:dynamic clj-decode enc/clj-decode-smile)

(defn position [^Buffer b] (.position b))
(defn set-position! [^Buffer b ^Integer i] (.position b i))

(defn clj->cassandra [cl]
  (let [rm (rename-keys cl {:stream-name :stream_name
                            :event-type :event_type
                            :caused-by :caused_by
                            :caused-by-relation :caused_by_relation
                            :service-id :service_id
                            :order-id :order_id
                            :event-time :event_time
                            :schema :schema_url})]
    (assoc rm :payload (clj-encode (:payload cl)))))

(defn cassandra->clj [cass]
  (log/trace (pr-str cass))
  (let [rm (rename-keys cass {:stream_name :stream-name
                              :event_type :event-type
                              :caused_by :caused-by
                              :caused_by_relation :caused-by-relation
                              :service_id :service-id
                              :event_time :event-time
                              :order_id :order-id
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
    (let [valid-seqs (remove #(nil? (first %)) seqs)
          sorted (sort-by #(sort-fn (first %)) valid-seqs)
          chosen (first sorted)
          new-seqs (cons (rest chosen) (rest sorted))]
      (cons (first chosen)
            (lazy-seq (ordered-combination sort-fn new-seqs))))))

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
      (cql/drop-keyspace conn (kspace conf))
      (cql/create-keyspace conn (kspace conf)
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
      (cql/insert conn (table conf) (clj->cassandra payload))))
  (db/distinct-values [this k]
    (let [conn (connection conf)
          ck (get {:stream-name :stream_name
                   :event-type :event_type
                   :caused-by :caused_by
                   :caused-by-relation :caused_by_relation
                   :service-id :service_id
                   :order-id :order_id
                   :event-time :event_time
                   :schema :schema_url}
                  k k)]
      (map :dv (cql/select conn (table conf)
                           (columns (-> ck distinct* (as "dv")))))))
  (db/lazy-events [this stream-name date]
    (log/info "Retrieving events from" stream-name "from date" date)
    (if (or (= "__all__" stream-name)
            (= :__all__ stream-name))
      (let [sts (db/distinct-values this :stream-name)]
        (ordered-combination :order-id
                             (map #(db/lazy-events this % date) sts)))
      (let [conn (connection conf)]
        (let [date-mms (* 1000 date)
              res (cql/select conn (table conf)
                              (where [[= :stream_name stream-name]
                                      [>= :order_id date-mms]])
                              (order-by [:order_id :asc])
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
                            (limit (chunk-size conf)))]
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

