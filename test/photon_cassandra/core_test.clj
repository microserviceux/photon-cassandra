(ns photon-cassandra.core-test
  (:require [clojure.test :refer :all]
            [photon.db-check :as check]
            [photon.db.cassandra :as cassandra]
            [photon.db :as db]))

(deftest db-check-test
  (let [impl (cassandra/->DBCassandra {:cassandra.ip "127.0.0.1"
                                       :kspace "photontest"
                                       :table "events"})]
    (db/delete-all! impl)
    (is (true? (check/db-check impl)))))
