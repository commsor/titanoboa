;; repo folder structure:
;;- /job-def-name (folder)
;;   |-HEAD                           (binary file - contains latest revision number)
;;   |-job-def-name.revisionumber.edn (file - head revision)
;;   |-job-def-name.revisionumber.edn (file - old(er) revision)
;;   |-job-def-name.revisionumber.edn (file - old(er) revision)
;;   |-job-def-name.revisionumber.edn (file - old(er) revision)

(ns titanoboa.repo
  (:require [clojure.edn :as edn]
            [me.raynes.fs :as fs]
            [titanoboa.exp :as exp]
            [titanoboa.dependencies :as deps]
            [clojure.tools.logging :as log]
            #_[clojure-watch.core :refer [start-watch]]
            [com.stuartsierra.component :as component])
  (:import [io.methvin.watcher DirectoryWatcher DirectoryChangeEvent DirectoryChangeListener DirectoryChangeEvent$EventType]))

(def lock (Object.))

(defn keyify [key maps-array]
  "Takes array of maps (of presumably same structure and converts them into map of maps using the provided key"
  (reduce #(merge %1 {(key %2) %2}) {} maps-array))

(defn read-job-def [edn-file]
  "Returns job definitions from specified edn file."
  (when-not (fs/exists? edn-file) (throw (java.io.FileNotFoundException. "The revision of the file was not found in repository!")))
  (let [content (edn/read-string
                  {:readers exp/edn-reader-map}
                  (slurp edn-file))]
    content))

(defn parse-revision [extension]
  "Parses file extension (e.g. '.001') into number (e.g. 1)"
  (-> (re-find  #"\d+" extension )
      Integer.))

(defn get-revision [f]
  (parse-revision (fs/extension (fs/name f))))

(defn list-revisions [def-folder]
  "Lists all available files in given directory.
  Returns map of results in format of {revision-number File}."
  (reduce
    #(assoc %1 (get-revision %2) %2)
    {}
    (fs/find-files def-folder #".*\.\d{3}\.edn")))

(defn get-head-rev [def-folder]
  "Finds and lists head revision of job definiton from given directory.
  Returns number of the head revision."
  (let [revisions-map (list-revisions def-folder)
        max-revision (if (empty? revisions-map)
                       0
                       (apply max (keys revisions-map)))]
    max-revision))

(defn read-head [def-folder]
  "Finds and reads head revision of job definiton from given directory.
  Returns tuple of version and job def map. If no revision is available returns [0 nil]. The job def map is automatically injected with :revision key/val"
  (let [revisions-map (list-revisions def-folder)
        max-revision (if (empty? revisions-map)
                       0
                       (apply max (keys revisions-map)))
        content (if (empty? revisions-map)
                  nil
                  (-> (get revisions-map max-revision)
                      read-job-def
                      (assoc :revision max-revision)))]
    [max-revision content]))

(defn list-head-defs [repo-folder]
  "Iterates through job def folders in provided repo folder and retrieves names and head revision numbers of all available job defs.
  Returns sequence of tuples in a format of [job-def-name reviesion]."
  (let [dir-seq (filter #(.isDirectory %) (fs/list-dir repo-folder))
        rev-seq (doall (map (fn [d] [(.getName d) (get-head-rev d)]) dir-seq))]
    rev-seq))

(defn get-revision-notes
  ([def-folder]
   (let [f (java.io.File. def-folder "HEAD")]
     (when (.exists f)
       (let [raf (java.io.RandomAccessFile. f "r")
             length (.length raf)
             last-rev-idx (if (> length 7 ) (.readLong raf) nil)]
         (loop [notes-map {}]
           (let [record  (try {(.readLong raf)
                               {:user (.readUTF raf)
                                :notes (.readUTF raf)}}
                              (catch java.io.EOFException e
                                nil))]
             (if record
               (recur (merge notes-map record))
               notes-map)))))))
  ([job-def repo-path]
   (let [def-name (name ((or key :name) job-def))
         def-folder (java.io.File. repo-path def-name)]
     (get-revision-notes def-folder))))

(defn list-all-revisions [repo-folder]
  "Iterates through job def folders in provided repo folder and retrieves names and head revision numbers of all available job defs.
  Returns sequence of vectors in a format of ([job-def-name [[revision Date user-name notes] [revision Date user-name notes]]])."
  (let [dir-seq (filter #(.isDirectory %) (fs/list-dir repo-folder))
        rev-notes (doall (reduce (fn [v d] (merge v {(.getName d) (get-revision-notes d)})) {} dir-seq))
        rev-seq (doall (map (fn [d] [(.getName d)
                                     (sort #(compare (first %2) (first %1)) (mapv (fn [[k v]] [k
                                                                                               (java.util.Date. (.lastModified v))
                                                                                               (get-in rev-notes [(.getName d) k :user])
                                                                                               (get-in rev-notes [(.getName d) k :notes])])
                                                                                  (list-revisions d)))])
                            dir-seq))]
    rev-seq))


(defn get-all-head-defs [repo-folder]
  "Iterates through job def folders in provided repo folder and retrieves head revisions of all available job defs.
  Returns sequence of job def maps - head revisions of job definitions from provided repo folder; their revisions are noted under :revision key."
  (let [dir-seq (filter #(.isDirectory %) (fs/list-dir repo-folder))
        def-seq (doall (map #(get (read-head %) 1) dir-seq))]
    def-seq))

(defn add-head2map [m]
  (let [head-num (->> (keys m)
                      (filter number?)
                      (apply max))]
    (assoc m :head (get m head-num))))


;;TODO load only released revisions
(defn get-all-revisions! [repo-folder]
  "Iterates through job def folders in provided repo folder and retrieves all available job defs.
  If there are any dependencies these are loaded into classloader-registry and registry index is provided.
  Note that due to mvn dependencies loading this method can take substantial amount of time to complete -
  depending on number of job definitions / steps that have dependencies, number of their revisions and the number of dependencies themselves5.
  Returns map of maps in a format of {job-def-name {revision-number {:job-def jobdefinition :cl-registry index}}}."
  (->> (fs/list-dir repo-folder)
       (filter #(.isDirectory %))
       (map (fn [i] {(.getName i) (->> (list-revisions i)
                                       (map (fn [[k v]] (let [jd (read-job-def v)]
                                                          [k {:job-def jd
                                                              #_:cl-registry #_(deps/load-jd-dependencies jd)}])))
                                       (into {})
                                       add-head2map)}))
       (reduce merge {})
       doall))

(defn get-head-def [repo-folder def-name]
  "Returns head revision of given job definition from given repository."
  (let [ [_ jd] (read-head (java.io.File. repo-folder def-name))]
    jd))

(defn get-def-rev [repo-folder def-name revision]
  "Returns specified revision of given job definition from given repository."
  (let [def-folder (java.io.File. repo-folder def-name)
        revisions-map (list-revisions def-folder) ;;TODO handle 404
        content (if-let [revision-file (get revisions-map revision)]
                  (read-job-def revision-file)
                  nil)]
    content))

#_(defn list-archive [def-folder]
  nil)
(defn lock-file! [f]
  "Locks file using JVM's lock on File System level.
  Takes a java.io.File as a parameter.
   Returns tuple of RandomAccessFile and lock objects."
  (let [raf (java.io.RandomAccessFile. f "rwd")
        file-ch (.getChannel raf)
        l (.lock file-ch)]
    [raf l]))

(defn lock-head-file! [def-folder]
  (lock-file! (java.io.File. def-folder "HEAD")))

(defn release-lock! [raf l]
 (.release l)
 (.close raf))

(defn save! [{:keys [def repo-path key revision-notes user]}]
  "Saves provided job-definition onto the file system as a new revision. Returns the newly assigned revision number.
  New revision number is calculated based on max file extension number in the job def folder.
  To obtain unique sequential revision number and avoid race conditions this method is synchronized locally (via locking)
  and also uses JVM's  lock on File System level (via a flag file named 'HEAD') so as multiple JVMs in a cluster would synchronize on this method as well."
  (locking lock
    (let [def-name (name ((or key :name) def))
          def-folder (java.io.File. repo-path def-name)
          _ (if-not (.exists def-folder) (.mkdirs def-folder))
          [raf l] (lock-head-file! def-folder)
          length (.length raf)
          idx (if (= length 0) 8 length)
          [rev _] (read-head def-folder)
          new-rev (inc rev)
          new-rev-ext (format "%03d" new-rev)
          new-rev-filename (str def-name "." new-rev-ext ".edn")
          new-rev-file (java.io.File. def-folder new-rev-filename)]
      (spit new-rev-file (assoc def :revision new-rev))
      (.seek raf 0)
      (.writeLong raf idx)
      (.seek raf idx)
      (.writeLong raf new-rev)
      (.writeUTF raf user)
      (.writeUTF raf revision-notes)
      (release-lock! raf l)
      new-rev)))


(defn watcher-callback [jd-atom filename]
  (when (re-matches #".*\.\d{3}\.edn" (.getName (java.io.File. filename)))
    (log/info "Detected Repository change - file: " filename )
    (try (let [jd (read-job-def filename)
               jd-name (:name jd)
               rev (:revision jd)]
           (log/debug "Repository change - jd name: " jd-name )
           (log/debug "Repository change - jd: " jd)
           (swap! jd-atom update jd-name
                  #(-> %
                       (assoc rev {:job-def jd
                                   #_:cl-registry #_(deps/load-jd-dependencies jd)})
                       add-head2map)))
         true
         (catch Exception e
           (log/warn e "Something went wrong during processing of a Repository change for file " filename " , retrying...")
           false))))

(defn callback-with-retry [f max-retry-count]
  "Retries function f if it returns false, or until max-retry-count is reached."
  (doall
    (take-while #(false? (%))
                (repeat max-retry-count f))))

(defrecord RepoWatcherComponent [folder-path jd-atom watcher]
  component/Lifecycle
  (start [this]
    (if watcher
      this
      (let [folder (java.io.File. folder-path)]
        (when-not (.exists folder) (.mkdirs folder))
        (let [watcher (-> (DirectoryWatcher/builder)
                        (.path (java.nio.file.Paths/get (.toURI folder)))
                        (.listener (reify io.methvin.watcher.DirectoryChangeListener
                                     (^void onEvent [this ^DirectoryChangeEvent event]
                                       (case (.name (.eventType event))
                                         "CREATE" (callback-with-retry (fn []
                                                                          (watcher-callback jd-atom (.toString (.path event))))
                                                                       5)
                                         "OVERFLOW" (log/info "Overflow event registered for repository file " (.toString (.path event)))
                                         nil))))
                        .build)]
          (log/info "Starting to watch repo folder for changes: " folder-path)
          (.watchAsync watcher)
          (assoc this :watcher watcher)))))
  (stop [this]
    (.close watcher)
    (dissoc this :watcher)))