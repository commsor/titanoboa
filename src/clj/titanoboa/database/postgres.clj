(ns titanoboa.database.postgres
  (:require [honeysql.core :as sql]
            [honeysql.helpers :as h]
            [clojure.java.jdbc :as jdbc]
            [taoensso.nippy :as nippy]
            [titanoboa.database :as db]))


(defmethod db/list-jobs "org.postgresql.Driver"
  [ds limit offset order]
  (let [query (cond->
                {:select [:jobid :jobdef :revision :state :start :ended :stepid :steptype :stepstate (sql/raw "count(*) OVER() AS totalcount")],
                 :from [:jobs]}
                limit (h/limit limit)
                offset (h/offset offset)
                order (h/order-by order)
                true sql/format)]
    (reduce (fn [val item]
              (-> (if (:totalcount val) val (assoc val :totalcount (:totalcount item)))
                  (update-in [:values] conj {:jobid (str (:jobid item))
                                             :jobdef {:name (:jobdef item)
                                                      :revision (:revision item)}
                                             :state (keyword (:state item))
                                             :start (:start item)
                                             :end (:ended item)
                                             :step-state (keyword  (:stepstate item))
                                             :step {:id  (:stepid item)
                                                    :type (keyword (:steptype item))}})))
            {:offset offset :limit limit :order order :values []}
            (jdbc/query ds query))))


#_(reduce (fn [val item]
          (-> (if (:totalcount val) val (assoc val :totalcount (:fullcount item)))
              (update-in [:values] conj (dissoc item :fullcount))))
        {:offset 0 :limit 10 :values []}
        (list-jobs (get-in @titanoboa.system/systems-state [:archival-system :system :db-pool]) 10 0 nil))