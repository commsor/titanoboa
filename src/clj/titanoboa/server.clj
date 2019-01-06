(ns titanoboa.server
  (:gen-class
    :implements [io.titanoboa.Startable])
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
            [titanoboa.system.local]
            [me.raynes.fs :as fs]
            [dynapath.util :as dp]
            [cemerick.pomegranate :as pom]
            [dynapath.dynamic-classpath :as dc])
  (:import [org.eclipse.jetty.server
            Server]
           [java.io File]
           io.titanoboa.cloader.DynamicClassLoader
           java.net.URLClassLoader))

(def ^Server server nil)

(defn find-ns-starting-with [s]
  (filter #(-> %
               str
               (.startsWith s)) (ns.find/find-namespaces (cp/classpath))))

(defn extract-zip-resource [resource unzip-to]
  (with-open [stream (-> (Thread/currentThread)
                         (.getContextClassLoader)
                         (.getResourceAsStream resource)
                         (java.util.zip.ZipInputStream.))]
    (loop [entry (.getNextEntry stream)]
      (if entry
        (let [savePath (str unzip-to java.io.File/separatorChar (.getName entry))
              saveFile (java.io.File. savePath)]
          (if (.isDirectory entry)
            (if-not (.exists saveFile)
              (.mkdirs saveFile))
            (let [parentDir (java.io.File. (.substring savePath 0 (.lastIndexOf savePath (int java.io.File/separatorChar))))]
              (if-not (.exists parentDir) (.mkdirs parentDir))
              (clojure.java.io/copy stream saveFile)))
          (recur (.getNextEntry stream)))))))

(defn init-step-repo! [path]
  (when-not (fs/exists? path)
    (-> path
        File.
        .mkdirs)
    (when (io/resource "steps-repo.zip")
      (extract-zip-resource "steps-repo.zip" path))))

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

(defn get-default-config [& [host]]
  {:host (or host (System/getProperty "boa.server.host") (.getHostAddress (java.net.InetAddress/getLocalHost)))
   :jetty {:port 3000
           :join? false}
   :steps-repo-path "steps-repo"
   :jobs-repo-path "repo"})

(def server-config {})

(defn get-node-id  [server-config]
  (str (:host server-config) ":" (get-in server-config [:jetty (if (get-in server-config [:jetty :ssl?]) :ssl-port :port)])))

(defn var->symbol [v]
  (symbol (str (:ns (meta v)))
          (str (:name (meta v)))))

(extend DynamicClassLoader
  dc/DynamicClasspath
  (assoc dc/base-readable-addable-classpath
    :add-classpath-url (fn [^DynamicClassLoader cl url]
                         (.addURL cl url))
    :classpath-urls #(seq (.getURLs ^URLClassLoader %))))

(defn load-dependencies! []
  (log/info "Classloader hierarchy:")
  (mapv #(log/info (str % " - modifiable: " (pom/modifiable-classloader? %))) (pom/classloader-hierarchy))
  (when-not (deps/get-deps-path-property)
    (deps/init-dependency-file!))
  (deps/start-deps-watch! (deps/get-deps-path-property)))

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

(defn init-job-folder! [path]
  (when-not (fs/directory? path) (fs/mkdirs path)))

(defn shutdown! []
  (println "Shutting down...")
  (log/info "Shutting down...")
  (.stop server)
  (deps/stop-deps-watch!)
  (system/stop-all-systems!))

(defn- shutdown-runtime![]
  (shutdown!)
  (shutdown-agents)
  (.interrupt @clojure.core.async.impl.timers/timeout-daemon))

(defn start
  [& [cfg host]]
   (log/info "Starting Titanoboa server...")
   (.addShutdownHook (Runtime/getRuntime) (Thread. shutdown-runtime!))
   (require-extensions)
   (load-dependencies!)
   (init-config! cfg host)
   (init-job-folder!  (:job-folder-path server-config))
   (init-step-repo! (:steps-repo-path server-config))
   (system/run-systems-onstartup! (:systems-catalogue server-config) server-config)
   (log/info "Starting jetty on port " (get-in server-config [:jetty (if (get-in server-config [:jetty :ssl?]) :ssl-port :port)]))
   (alter-var-root #'server
                   (constantly (run-jetty (handler/get-ring-app server-config)
                                          (:jetty server-config)))))
(defn -start [this]
  (start))

(defn -main [& args]
  (start))
