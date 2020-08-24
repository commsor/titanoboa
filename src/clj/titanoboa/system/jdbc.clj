; Copyright (c) Miroslav Kubicek. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.system.jdbc
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [titanoboa.database :as db])
  (:import [com.mchange.v2.c3p0 ComboPooledDataSource]))


(defn jdbc-pool [config]
  (component/system-map
    :pool (db/map->JdbcPoolComponent {:config (merge
                                             {:jdbc-url "jdbc:postgresql://localhost:5432/mydb"
                                              :user "postgres"
                                              :password "postgres"
                                              :driver-class "org.postgresql.Driver"
                                              :minimum-pool-size 2
                                              :maximum-pool-size 10
                                              :excess-timeout (* 30 60)
                                              :idle-timeout (* 20 60)}
                                             config)})))

#_(vec (jdbc/query (:pool (:system (:db @titanoboa.system/systems-state)))
                 (sql/format {:select [:*]
                              :from [:customers]})))