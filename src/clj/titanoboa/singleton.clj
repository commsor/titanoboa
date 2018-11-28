(ns titanoboa.singleton
  (:require [clojure.tools.logging :as log]))

(def singletons (atom {}))

(defn get-instance [c & args]
  (or (get @singletons c)
      (do (log/info "Instantiating a singleton bean " c)
          (get (swap! singletons assoc c (clojure.lang.Reflector/invokeConstructor c (into-array Object args)))
               c))))
