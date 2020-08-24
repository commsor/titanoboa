; Copyright (c) Miroslav Kubicek. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.system.auth
  (:require [titanoboa.database :as db]
            [com.stuartsierra.component :as component]))

(defn auth-system [config]
  (component/system-map
    :db-pool (db/map->JdbcPoolComponent {:config (merge
                                                   {:jdbc-url "jdbc:postgresql://localhost:5432/mydb?currentSchema=titanoboa"
                                                    :user "postgres"
                                                    :password "postgres"
                                                    :driver-class "org.postgresql.Driver"
                                                    :minimum-pool-size 2
                                                    :maximum-pool-size 15
                                                    :excess-timeout (* 30 60)
                                                    :idle-timeout (* 3 60 60)}
                                                   config)})))