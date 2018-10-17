(ns titanoboa.server
  (:gen-class)
  (:require [clojure.repl]
            [clojure.tools.namespace.find :as ns.find]
            [clojure.java.io :as io]
            [clojure.java.classpath :as cp]
            [titanoboa.handler :as handler]
            [ring.adapter.jetty :refer [run-jetty]]
            [titanoboa.system :as system]
            [titanoboa.dependencies :as deps]
            [titanoboa.api :as api]
            [titanoboa.channel :as channel]
            [titanoboa.database :as db]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [titanoboa.system.local])
  (:import [org.eclipse.jetty.server
            Server]))

(def ^Server server nil)

(defn process-cluster-command [c]
  (eval c))

(defn find-ns-starting-with [s]
  (filter #(-> %
               str
               (.startsWith s)) (ns.find/find-namespaces (cp/classpath))))

(defn require-ns [ns]
  (try
    (log/info "Trying to require namespace " ns )
    (require ns)
    (log/info "Successfully required namespace" ns )
    (catch Exception e
      (log/error e "Failed to require namespace" ns))))

(defn require-extensions []
  (log/info "Loading database extensions...")
  (mapv require-ns (find-ns-starting-with "titanoboa.database."))
  (log/info "Loading system definitions...")
  (mapv require-ns (find-ns-starting-with "titanoboa.system."))
  (log/info "Loading tasklet definitions...")
  (mapv require-ns (find-ns-starting-with "titanoboa.tasklet.")))

;;FIXME validate size of the async thread pool - revise all usages of thread macro (some may use standalone Thread)!
;;TODO also size needed will depend on whether or not AWS/SQS is used, as the polling and acknowledgement and alt operations all use threading macro
(System/setProperty "clojure.core.async.pool-size" "24")

(defn get-default-config [& [host]] {:host (or host (System/getProperty "boa.server.host") (.getHostAddress (java.net.InetAddress/getLocalHost)))
                     :jetty {:port 3000
                             :join? false}})

(def server-config {})

(defn get-node-id  [server-config]
  (str (:host server-config) ":" (get-in server-config [:jetty (if (get-in server-config [:jetty :ssl?]) :ssl-port :port)])))

(defn var->symbol [v]
  (symbol (str (:ns (meta v)))
          (str (:name (meta v)))))

(defn load-dependencies! []
  (if-let [config-path (deps/get-deps-path-property)]
    (deps/start-deps-watch! config-path)
    (log/warn "No external dependencies configuration found. No dependencies will be loaded!")))

(defn init-config! [& [cfg host]]
  (if cfg
    (alter-var-root #'server-config (constantly cfg))
    (if-let [config-path (System/getProperty "boa.server.config.path")]
      (load-file config-path)
      (if-let [cp-config (io/resource "boa-server-config.clj")]
        (load-string (slurp cp-config))
        (throw (IllegalStateException. "Titanoboa server config file was not found. It should be either on classpath (as \"boa-server-config.clj\") or its path should be denoted by \"boa.server.config.path\" system property.")))))
  (alter-var-root #'server-config #(merge (get-default-config host) %))
  (alter-var-root #'server-config assoc :node-id (get-node-id server-config))
  (alter-var-root #'server-config assoc :systems-catalogue (into {}
                                                                 (mapv (fn [[k v]]
                                                                         [k (merge v {:system-def-source (clojure.repl/source-fn (var->symbol (:system-def v)))};;FIXME this does not seem to work in uberjar
                                                                                   (when (:worker-def v) {:worker-def-source (clojure.repl/source-fn (var->symbol (:worker-def v)))}))])
                                                                       (:systems-catalogue server-config)))))

;;TODO revise how catalogue is being stored/accessed? Seems it is inevitable to store it in some atom or var since it needs to be accessed arbitrarily from different places?
;;(system/set-system-catalogue! (:systems-catalogue server-config))

(defn shutdown! []
  (println "Shutting down...")
  (log/info "Shutting down...")
  (.stop server)
  (try (deps/stop-deps-watch!))
  (try (system/stop-all-systems!)))

(defn- shutdown-runtime![]
  (shutdown!)
  (try (shutdown-agents))
  (try (.interrupt clojure.core.async.impl.timers/timeout-daemon)))

(defn -main [& [cfg host]]
  (log/info "Starting Titanoboa server...")
  (.addShutdownHook (Runtime/getRuntime) (Thread. shutdown-runtime!))
  (require-extensions)
  (load-dependencies!)
  (init-config! cfg host)
  (system/run-systems-onstartup! (:systems-catalogue server-config) server-config)
  (log/info "Starting jetty on port " (get-in server-config [:jetty (if (get-in server-config [:jetty :ssl?]) :ssl-port :port)]))
  (alter-var-root #'server
                  (constantly (run-jetty (handler/get-ring-app server-config)
                                         (:jetty server-config)))))

;;comment this out if NOT running server w/ figwheel or REPL:
#_(do
  (log/info "Starting Titanoboa server...")
  (require-extensions)
  (load-dependencies!)
  (init-config!)
  (def app (handler/get-ring-app server-config))
  (system/run-systems-onstartup! (:systems-catalogue server-config) server-config))