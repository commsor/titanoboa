(ns titanoboa.dependencies
  (:require [cemerick.pomegranate.aether :as aether]
            [cemerick.pomegranate :as pom]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [clojure-watch.core :refer [start-watch]]
            [me.raynes.fs :as fs])
  (:import (java.io File FileOutputStream)))

(def dependencies-path-property "boa.server.dependencies.path")

(defn get-deps-path-property []
  (System/getProperty dependencies-path-property))

(def repl? false)
(def lock (Object.))

(def ext-deps-system nil)
(def classloader-registry (atom [])) ;;[[dependency-map classloader]]

;;#_{job-def-name {revision-number {:cl-registry {step-id classloader-idx}
;;                                :job-def job-def-map}}}


(defn get-base-cl []
  (-> @classloader-registry
      first
      second))

(defn get-deps-map [& coordinates] ;;[(symbol "ring-middleware-format") "0.7.0"]
  (into {}
      (mapv
        (fn [[k v]] [(first k) (second k)])
        (aether/resolve-dependencies :coordinates coordinates))))

(defn get-conflicts [& dep-maps]
  (->> (apply merge-with (fn [v1 v2] (if (= v1 v2)
                            v1
                            (if (vector? v1)
                              (conj v1 v2)
                              [v1 v2])))
         dep-maps)
  (filter (fn [[k v]] (vector? v)))))

(defn dep-subset? [current-deps-map new-deps-map]
  (clojure.set/subset? (into #{} new-deps-map) (into #{} current-deps-map)))

(defn get-new-classloader []
  (if repl?
    @clojure.lang.Compiler/LOADER ;;@clojure.lang.Compiler/LOADER ;;(.getClassLoader clojure.lang.RT)
    (-> (if (.isBound clojure.lang.Compiler/LOADER) @clojure.lang.Compiler/LOADER
                                                    (if *use-context-classloader*
                                                      (.getContextClassLoader (Thread/currentThread))
                                                      (.getClassLoader clojure.lang.RT)))
        (clojure.lang.DynamicClassLoader.))))

(defn init-classloaders-registry! [init-deps-coordinates]
  (locking lock
    (when-not (empty? @classloader-registry) (throw (IllegalStateException. "classloader-registry has been already initialized!")))
    (reset! classloader-registry [[(apply get-deps-map init-deps-coordinates) (get-new-classloader)]])))

(defn find-dep-subset [coordinates]
  "Without retrieving any dependencies via maven, this fn just scans through classloader-registry to see whether there is a obvious match for provided coordinates.
  Returns tuple of [index classloader] or nil if no match is found."
  (let [deps-map (into {} coordinates)]
    (some->> @classloader-registry
             (keep-indexed (fn [idx itm]
                             (when (dep-subset? (get itm 0) deps-map)
                               [idx (get itm 1)])))
             first)))

;;FIXME rewrite this to also instantiate a separate RT for each classloader at the end - and then run all requires/imports in the RT (these should come also in JD along with dependencies) - OTHERWISE THIS WONT WORK!
;;TODO add support for maven repositories
(defn add-dependencies! [coordinates]
  "Resolves all dependencies of provided dependency coordinates. Then iterates through the classloader-registry and uses first classloader possible to add these dependencies.
  If no classloader could be used a new one is created. Also corresponding dependencies map is updated in classloader-registry.
  Returns classloader-registry index pointing to the classloader that was used."
  (if-let [[straight-match-idx _] (find-dep-subset coordinates)]
    straight-match-idx
    (locking lock
      (if (empty? @classloader-registry)
        (throw (IllegalStateException. "classloader-registry has not been initialized!"))
        (let [new-deps-map (apply get-deps-map coordinates)]
          (log/info "Adding dependencies for coordinates: " coordinates)
          (loop [i 0]   ;;let [ (map-indexed (fn [idx itm] (when (dep-subset? (first itm) new-deps-map) idx)) @classloader-registry)]
            (let [[deps-map classloader] (get @classloader-registry i)]
              (cond
                (dep-subset? deps-map new-deps-map) (do
                                                      (log/info "Matching classloader found in registry [" i "] no need to load dependencies.")
                                                      i)
                repl? (do
                        (when-not (zero? i) (throw (IllegalStateException. "Multiple classloaders found in classloader-registry. In REPL mode there should be only one classloader!")))
                        (log/info "Adding dependencies into (REPL) classloader registry [" i "]...")
                        (pom/add-dependencies :coordinates coordinates
                                              :classloader classloader)
                        (swap! classloader-registry update-in [i 0] merge new-deps-map)
                        i)
                (empty? (get-conflicts new-deps-map deps-map)) (do
                                                                 (log/info "Adding dependencies into classloader registry [" i "]...")
                                                                 (pom/add-dependencies :coordinates coordinates
                                                                                       :classloader classloader)
                                                                 (swap! classloader-registry update-in [i 0] merge new-deps-map)
                                                                 i)
                (>= (inc i) (count @classloader-registry)) (let [new-cl (get-new-classloader)]
                                                             (log/info "Adding dependencies into a new classloader; registry: [" (inc i) "]...")
                                                             (pom/add-dependencies :coordinates coordinates
                                                                                   :classloader new-cl)
                                                             (swap! classloader-registry conj [new-deps-map new-cl])
                                                             (inc i))
                :else (recur (inc i))))))))))

(defn dep-strings->symbols [dependencies]
  (mapv (fn [[d v]]
          [(symbol d) v])
        dependencies))

(defn load-jd-dependencies [jd]
  "Loads dependencies for all steps of given job definition that require any (i.e. that contain any :dependencies)
  Returns a map of {:stp-id index-from-registry-where-dependencies-were-added}"
  (some->> jd
      :steps
       (filter :dependencies)
       (map (fn [i] [(:id i) (add-dependencies! (mapv (fn [[d v]]
                                                  [(symbol d) v])
                                                (:dependencies i)))]))
       (into {})))


(defmacro with-classloader [cl & body]
  `(binding [*use-context-classloader* true]
     (let [cl# (.getContextClassLoader (Thread/currentThread))]
       (try (.setContextClassLoader (Thread/currentThread) ~cl)
            (clojure.lang.Var/pushThreadBindings {clojure.lang.Compiler/LOADER new-cl})
            ~@body
            (finally
              (clojure.lang.Var/popThreadBindings)
              (.setContextClassLoader (Thread/currentThread) cl#))))))

(defn load-ext-dependencies [ext-coordinates]
  "Loads provided dependencies (in bulk) and requires/imports specified namespaces/classes.
  ext-coordinates is supposed to be in a format of {:coordinates [[com.draines/postal \"2.0.2\"]] :require [[postal.core]] :import nil :repositories nil}"
  (when-let [c (:coordinates ext-coordinates)]
    (log/info "Loading external dependencies: \n" c " \n from repositories " (:repositories ext-coordinates))
    (pom/add-dependencies :coordinates c
                    :repositories (:repositories ext-coordinates))
    (when-let [r (:require ext-coordinates)]
      (log/info "Requiring external namespaces: " r)
      (apply require r))
    (when-let [i (:import ext-coordinates)]
      (log/info "Importing external classes: " i)
      (when (sequential? i) (mapv #(import %) i)))))

 ;;TODO there is no locking of the deps file!
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

#_(defn test0 []
  (let [original-cl (.getContextClassLoader (Thread/currentThread))
        new-cl (get-new-classloader)]
    (log/info "*use-context-classloader* is " *use-context-classloader*) ;;@clojure.lang.Compiler/LOADER
    (log/info "@clojure.lang.Compiler/LOADER is " @clojure.lang.Compiler/LOADER)
    (log/info "current context classloader is  " (.getContextClassLoader (Thread/currentThread)))
    (log/info "loading postal into cl " new-cl)
    (log/info "@clojure.lang.Compiler/LOADER is " @clojure.lang.Compiler/LOADER)
    (cemerick.pomegranate/add-dependencies :coordinates [['com.draines/postal "2.0.2"]]
                                           :classloader new-cl)
    (with-classloader new-cl
                      (clojure.lang.Var/pushThreadBindings {clojure.lang.Compiler/LOADER new-cl})
                      (.setContextClassLoader (Thread/currentThread) new-cl)
                      (log/info "current context classloader is  " (.getContextClassLoader (Thread/currentThread)))
                      (require 'postal.core)
                      [new-cl original-cl])))

#_(defn test1 []
  (binding [clojure.core/*use-context-classloader* true]
    (let [original-cl (.getContextClassLoader (Thread/currentThread))
          new-cl (get-new-classloader)]
      (try
        (log/info "*use-context-classloader* is " *use-context-classloader*) ;;@clojure.lang.Compiler/LOADER
        (log/info "@clojure.lang.Compiler/LOADER is " @clojure.lang.Compiler/LOADER)
        (log/info "current context classloader is  " (.getContextClassLoader (Thread/currentThread)))
        (log/info "loading postal into cl " new-cl)
        (log/info "@clojure.lang.Compiler/LOADER is " @clojure.lang.Compiler/LOADER)
        (cemerick.pomegranate/add-dependencies :coordinates [['com.draines/postal "2.0.2"]]
                                               :classloader new-cl)
        (clojure.lang.Var/pushThreadBindings {clojure.lang.Compiler/LOADER new-cl})
        (.setContextClassLoader (Thread/currentThread) new-cl)
        (log/info "current context classloader is  " (.getContextClassLoader (Thread/currentThread)))
        (require 'postal.core)
        [new-cl original-cl]
        (finally (.setContextClassLoader (Thread/currentThread) original-cl)
                 (clojure.lang.Var/popThreadBindings))))))

#_(defn test2 []
  (binding [clojure.core/*use-context-classloader* true]
    (let [original-cl (.getContextClassLoader (Thread/currentThread))
          new-cl (get-new-classloader)]
      (try
        (log/info "*use-context-classloader* is " *use-context-classloader*) ;;@clojure.lang.Compiler/LOADER
        (log/info "@clojure.lang.Compiler/LOADER is " @clojure.lang.Compiler/LOADER)
        (log/info "current context classloader is  " (.getContextClassLoader (Thread/currentThread)))
        (log/info "loading postal into cl " new-cl)
        (log/info "@clojure.lang.Compiler/LOADER is " @clojure.lang.Compiler/LOADER)
        (cemerick.pomegranate/add-dependencies :coordinates [['com.draines/postal "2.0.2"]]
                                               :classloader new-cl)
        (.setContextClassLoader (Thread/currentThread) new-cl)
        (log/info "current context classloader is  " (.getContextClassLoader (Thread/currentThread)))
        (require 'postal.core)
        [new-cl original-cl]
        (finally (.setContextClassLoader (Thread/currentThread) original-cl))))))

#_{:coordinates [[com.draines/postal "2.0.2"]
               [com.github.kyleburton/clj-xpath "1.4.10"]]
 :require [[postal.core]
           [clj-xpath.core]]
 :import nil
 :repositories {"central" "https://repo1.maven.org/maven2/"
                "clojars" "https://clojars.org/repo"}}