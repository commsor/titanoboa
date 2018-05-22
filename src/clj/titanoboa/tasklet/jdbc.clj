(ns titanoboa.tasklet.jdbc
  (:require [honeysql.core :as sql]
            [clojure.java.jdbc :as jdbc]
            [titanoboa.system]))

(defn query [{{:keys [response-property-name data-source-ks query] :as properties} :properties :as job}]
  (let [q (cond
            (vector? query) query
            (map? query) (sql/format query))
        ds (get-in @titanoboa.system/systems-state data-source-ks)]
    {:properties
     {response-property-name  (vec (jdbc/query ds q))}}))