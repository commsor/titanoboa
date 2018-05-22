(ns titanoboa.tasklet.httpclient
  (:require [clj-http.client :as client]))

(defn request [{{:keys [response-property-name url body-only? connection-pool] :as properties} :properties :as job}]
  (let [response (cond
                   (string? url) (if body-only? (:body (client/request properties)) (client/request properties))
                   (vector? url) (client/with-connection-pool (merge {:timeout 5 :threads 4 :insecure? false :default-per-route 10} connection-pool)
                                                       (reduce
                                                         (fn [val item]
                                                           (let [r (client/request (assoc properties :url item))]
                                                            (conj val (if body-only? (:body r) r))))
                                                         []
                                                         url))
                   :else (throw (IllegalArgumentException. "URL can be either a string or vector of strings")))]
    {:properties
     {response-property-name response}}))