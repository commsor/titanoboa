(ns titanoboa.tasklet.smtp
  (:require [postal.core :as postal]))


(defn send-email [{:keys [connection email] :as properties}]
  (let [result-map (postal.core/send-message connection email)]
    (if (= :SUCCESS (:error result-map))
      {:exit (:code result-map)
       :properties {:smtp-result result-map}}
      result-map)))