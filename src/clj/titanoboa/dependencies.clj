(ns titanoboa.dependencies
  (:require [cemerick.pomegranate.aether :as aether]
            [cemerick.pomegranate :as pom]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [clojure-watch.core :refer [start-watch]]
            [me.raynes.fs :as fs]
            [clojure.java.io :as io]
            [dynapath.util :as dp]
            [dynapath.dynamic-classpath :as dc]
            [compliment.utils]
            [titanoboa.exp :as exp]
            [clojure.walk :as walk])
  (:import (java.io File FileOutputStream)))

(def dependencies-path-property "boa.server.dependencies.path")

(def empty-deps {:coordinates []
                 :require []
                 :import nil
                 :repositories {"central" "https://repo1.maven.org/maven2/"
                                "clojars" "https://clojars.org/repo"}})

(defn init-dependency-file! []
  (let [deps-file (File. "ext-dependencies.clj")]
    (if (io/resource "ext-dependencies.clj")
      (try
        (log/info "Initialization: Copying external dependencies from read-only file on classpath...")
        (spit deps-file (slurp (io/resource "ext-dependencies.clj")))
        (System/setProperty dependencies-path-property (.getAbsolutePath deps-file))
        (catch Exception e
          (log/error e "Error loading external dependencies from file on classpath!")))
      (do (log/warn "No external dependencies configuration found. creating empty dependencies file...")
          (spit deps-file (slurp (str empty-deps)))
          (System/setProperty dependencies-path-property (.getAbsolutePath deps-file))))))

(defn get-deps-path-property []
  (System/getProperty dependencies-path-property))

(def lock (Object.))

(def ext-deps-system nil)

#_(defn effectively-empty? [col]
  "Returns true is collection is empty or contains empty collections. returns also true if not a sequence."
  (or (not (sequential? col)) (empty? col) (when (sequential? col)
                        (and (every? sequential? col)
                             (every? empty? col)))))

(defn effectively-empty? [col]
  (or (empty? (flatten col))
      (every? #(or (= "" %) (nil? %)) (flatten col))))

(defn normalize-imports [imports norm-fn]
  (when imports
    (let [norm-fn (or norm-fn constantly)
        imports (walk/prewalk exp/eval-property imports)]
  (cond
    (and (sequential? imports) (not (effectively-empty? imports))) (map #(norm-fn %) (flatten imports))
    (not (sequential? imports)) [(norm-fn imports)]
    :else nil))))

(defn load-ext-dependencies [ext-coordinates]
  "Loads provided dependencies (in bulk) and requires/imports specified namespaces/classes.
  ext-coordinates is supposed to be in a format of {:coordinates [[com.draines/postal \"2.0.2\"]] :require [[postal.core]] :import nil :repositories nil}"
  (when-let [c (:coordinates ext-coordinates)]
    (when-not (effectively-empty? c)
      (log/info "Loading external dependencies: \n" c " \n from repositories " (:repositories ext-coordinates))
      (pom/add-dependencies :coordinates c
                          :repositories (:repositories ext-coordinates)
                          :classloader (last (filter pom/modifiable-classloader?
                                                     (pom/classloader-hierarchy)))))) ;;:classloader (clojure.lang.RT/baseLoader)
  (when-let [r (:require ext-coordinates)]
    (when-not (effectively-empty? r)
    (log/info "Requiring external namespaces: " r)
    (apply require r))) ;;TODO maybe require them in titanoboa.exp (or other dedicated) namespace?
  (when-let [i (normalize-imports (:import ext-coordinates) symbol)]
    (log/info "Importing external classes: " i) ;;TODO import them in titanoboa.exp (or other dedicated) namespace!
       (when (and (sequential? i) (not (empty? i))) (let [nspace (find-ns 'titanoboa.exp)]
                                                              (mapv #(.importClass nspace (resolve %)) i))))
  (log/info "Finished loading external dependencies.")
  (compliment.utils/flush-caches)
  (titanoboa.exp/init-java-lambda-factory! (.getContextClassLoader (Thread/currentThread)) (normalize-imports (:import ext-coordinates) str)))

;;TODO there might be need for retry in case the file stays locked for longer?
(defrecord DepsWatcherComponent [deps-file-path stop-callback-fn last-content-atom]
  component/Lifecycle
  (start [this]
    (if stop-callback-fn
      this
      (do
        (when-not (fs/exists? deps-file-path) (fs/create (File. deps-file-path)))
        (assoc this
          :stop-callback-fn (start-watch [{:path       (if (fs/directory? deps-file-path) deps-file-path (-> deps-file-path
                                                                                                             fs/parent
                                                                                                             (.getAbsolutePath)))
                                           :event-types [:modify]
                                           :bootstrap   (fn [path]
                                                          (let [coordinates (try
                                                                              (read-string (slurp deps-file-path))
                                                                              (catch Exception e
                                                                                (log/error e "Error reading external dependencies file: does not exist or has invalid format.")
                                                                                nil))]
                                                            (log/info "Initialization: Loading external dependencies from " path)
                                                            (reset! last-content-atom coordinates)
                                                            (try (load-ext-dependencies coordinates)
                                                                 (catch Exception e
                                                                   (log/error e "Error loading external dependencies!")))
                                                            (log/info "Starting to watch external dependencies file for changes: " path)))
                                           :callback    (fn [event filename]
                                                          (when (= (File. filename) (File. deps-file-path))
                                                            (locking lock
                                                              (let [coordinates (try
                                                                                  (read-string (slurp filename))
                                                                                  (catch Exception e
                                                                                    (log/error e "Error reading external dependencies file: does not exist or has invalid format.")
                                                                                    nil))]
                                                                (when (and coordinates (map? coordinates) (not= coordinates @last-content-atom))
                                                                  (log/info "Detected external dependencies change - event: " event " file: " filename " ; Reloading...")
                                                                  (try (load-ext-dependencies coordinates)
                                                                       (reset! last-content-atom coordinates)
                                                                       (catch Exception e
                                                                         (log/error e "Error loading external dependencies!"))))))))
                                           :options     {:recursive false}}])))))
  (stop [this]
    (stop-callback-fn)
    (dissoc this :stop-callback-fn)))


(defn new-watcher-system [{:keys [ext-deps-file] :as config}]
  (component/system-map :deps-watcher (map->DepsWatcherComponent {:deps-file-path ext-deps-file
                                                                  :last-content-atom (atom nil)})))

(defn start-deps-watch! [path]
  (locking lock
    (if-not ext-deps-system
      (do
        (alter-var-root #'ext-deps-system (constantly (new-watcher-system {:ext-deps-file path})))
        (alter-var-root #'ext-deps-system component/start))
      (log/warn "External Dependencies monitoring system has already been initialized!"))))

(defn stop-deps-watch! []
  (locking lock
    (when ext-deps-system
      (alter-var-root #'ext-deps-system component/stop))))

(defn get-deps-file-content []
  (slurp (get-deps-path-property)))

(defn lock-file! [f]
  "Locks file using JVM's lock on File System level.
  Takes a java.io.File as a parameter.
   Returns tuple of RandomAccessFile and lock objects."
  (let [raf (java.io.RandomAccessFile. f "rwd")
        file-ch (.getChannel raf)
        l (.lock file-ch)]
    [raf l]))

(defn release-lock! [raf l]
  (.release l)
  (.close raf))

(defn write-deps-file! [old-content new-content]
  (locking lock
    (let [f (get-deps-path-property)
          [raf l] (lock-file! (java.io.File. (fs/parent f) "deps-lock"))
          curr-content (get-deps-file-content)
          stale? (not= old-content curr-content)]
      (try
        (when-not stale? ;;(throw (IllegalStateException. "The file you are trying to change has been changed by other user in the meantime!")))
          (spit f new-content)) ;;FIXME can I use spit with os opened? if not i would have to send .getBytes on string content to the os
        (not stale?)
        (finally
          (release-lock! raf l))))))

(defn log-cl-hierarchy []
  (when (log/enabled? :info)
    (log/info "Classloader hierarchy:")
    (mapv #(log/info (str % " - modifiable: " (pom/modifiable-classloader? %))) (pom/classloader-hierarchy))))
