; Copyright (c) Commsor Inc. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.database.postgres
  (:require [honeysql.core :as sql]
            [honeysql.helpers :as h]
            [clojure.java.jdbc :as jdbc]
            [taoensso.nippy :as nippy]
            [titanoboa.database :as db]
            [clojure.edn :as edn]))


(defmethod db/list-jobs "org.postgresql.Driver"
  [ds limit offset order]
  (let [query (cond->
                {:select [:jobid :jobdef :revision :state :start :ended :stepid :steptype :stepstate :isparent :parentjobid :threadstack (sql/raw "count(*) OVER() AS totalcount")],
                 :from [:jobs]}
                limit (h/limit limit)
                offset (h/offset offset)
                order (h/order-by order)
                true sql/format)]
    (reduce (fn [val item]
              (-> (if (:totalcount val) val (assoc val :totalcount (:totalcount item)))
                  (update-in [:values] conj {:jobid        (str (:jobid item))
                                             :parent-jobid (when (:parentjobid item) (str (:parentjobid item)))
                                             :jobdef       {:name     (:jobdef item)
                                                            :revision (:revision item)}
                                             :isparent?    (:isparent item)
                                             :state        (keyword (:state item))
                                             :start        (:start item)
                                             :end          (:ended item)
                                             :step-state   (keyword (:stepstate item))
                                             :step         {:id   (:stepid item)
                                                            :type (keyword (:steptype item))}
                                             :thread-stack (edn/read-string (:threadstack item))})))
            {:offset offset :limit limit :order order :values []}
            (jdbc/query ds query))))


#_(reduce (fn [val item]
          (-> (if (:totalcount val) val (assoc val :totalcount (:fullcount item)))
              (update-in [:values] conj (dissoc item :fullcount))))
        {:offset 0 :limit 10 :values []}
        (list-jobs (get-in @titanoboa.system/systems-state [:archival-system :system :db-pool]) 10 0 nil))