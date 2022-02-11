; Copyright (c) Commsor Inc. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.database
  (:require [honeysql.core :as sql]
            [honeysql.helpers :as h]
            [clojure.java.jdbc :as jdbc]
            [taoensso.nippy :as nippy]
            [clj-time.coerce :as t]
            [taoensso.nippy :as nippy]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [titanoboa.channel :as ch])
  (:import [com.mchange.v2.c3p0 ComboPooledDataSource]))

(defn connection-pool
  [{:keys [jdbc-url driver-class user password
           excess-timeout idle-timeout minimum-pool-size maximum-pool-size
           test-connection-query
           idle-connection-test-period
           test-connection-on-checkin
           test-connection-on-checkout]
    :or {excess-timeout (* 30 60)
         idle-timeout (* 3 60 60)
         minimum-pool-size 3
         maximum-pool-size 15
         test-connection-on-checkin false
         test-connection-on-checkout false}
    :as config}]
  (doto (ComboPooledDataSource.)
    (.setDriverClass driver-class)
    (.setJdbcUrl jdbc-url)
    (.setUser user)
    (.setPassword password)
    (.setMaxIdleTimeExcessConnections excess-timeout)
    (.setMaxIdleTime idle-timeout)
    (.setMinPoolSize minimum-pool-size)
    (.setMaxPoolSize maximum-pool-size)
    (.setTestConnectionOnCheckin test-connection-on-checkin)
    (.setTestConnectionOnCheckout test-connection-on-checkout)))

;;FIXME move hese funcitons to a separate "component" namespace (or move the system def away)
(defrecord JdbcPoolComponent [datasource config]
  component/Lifecycle
  (start [this]
    (log/info "Initializing  Jdbc Connection Pool Component...")
    (assoc this :datasource (connection-pool config)))
  (stop [this]
    (.close datasource)
    this))

(defn execute! [datasource command]
  (jdbc/execute! datasource command))

(defn archive-job! [ds job]
  (jdbc/insert! ds "jobs" {:jobid     (java.util.UUID/fromString (:jobid job))
                           :jobdef    (get-in job [:jobdef :name])
                           :revision  (get-in job [:jobdef :revision])
                           :state     (name (:state job))
                           :start     (t/to-sql-time (:start job))
                           :ended     (t/to-sql-time  (:end job))
                           :stepstate (name (:step-state job))
                           :stepid    (get-in job [:step :id])
                           :steptype  (name (get-in job [:step :type]))
                           :isparent  (:isparent? job)
                           :parentjobid (when (:parent-jobid job) (java.util.UUID/fromString (:parent-jobid job)))
                           :threadstack (prn-str (:thread-stack job))
                           :job       (nippy/freeze job)}))

(defmulti list-jobs
          "Retrieve list of archived jobs. The jobs are not to be containing all data - only a short map with :jobid :jobdef :revision :state :start :ended :stepid :steptype :stepstate!
          Returns a map in a format of  {:offset offset :limit limit :totalcount total-number-of-rows-available :values [{job 1} {job 2} {job n}]}"
          (fn [ds limit offset order] (.getDriverClass (:datasource ds))))


(defn get-job [ds jobid]
  (some->> {:select [:job]
            :from [:jobs]
            :where [:= :jobid (java.util.UUID/fromString jobid)]}
           (sql/format)
           (jdbc/query ds)
           first
           :job
           (nippy/thaw)))

(defn get-jobs
  "retrieve a job including all the job's threads if they exist (usually only in case the job is suspended mid-run)"
  [ds jobid]
  (some->> {:select [:job]
            :from [:jobs]
            :where [:or [:= :jobid (java.util.UUID/fromString jobid)] [:= :parentjobid (java.util.UUID/fromString jobid)]]}
           (sql/format)
           (jdbc/query ds)
           (mapv :job)
           (mapv nippy/thaw)))

(defn delete-jobs [ds jobid]
  (jdbc/with-db-transaction [con ds]
                            (some->> {:delete-from :jobs
                                      :where [:or [:= :jobid (java.util.UUID/fromString jobid)] [:= :parentjobid (java.util.UUID/fromString jobid)]]}
                                     (sql/format)
                                     (jdbc/execute! con))))


(defrecord JobArchivingComponent [ds mq-session-comp finished-jobs-chan thread-handle]
  component/Lifecycle
  (start [this]
    (log/info "Starting JobArchivingComponent...")
    (if thread-handle
      this
      (let [th (Thread. (fn[]
                          (log/info "Starting JobArchivingComponent thread [" (.getName (Thread/currentThread)) "].")
                          (log/debug "Retrieving finished job for archival...")
                          (ch/with-mq-session mq-session-comp
                                              (loop [job (async/<!! finished-jobs-chan)]
                                                (log/info "Archiving finished job ["(:jobid job)"]...")
                                                (try (archive-job! ds job)
                                                     (ch/ack! finished-jobs-chan job)
                                                     (catch Exception e
                                                       (log/warn "JobArchivingComponent thread [" (.getName (Thread/currentThread)) "]: Exception during archival of job [" (:jobid job) "]: " e)))
                                                (log/debug "Retrieving finished job for archival...")
                                                (recur (async/<!! finished-jobs-chan))))))]
        (.start th)
        (assoc this
          :thread-handle th))))
  (stop [this]
    (log/info "Stopping JobArchivingComponent thread [" (.getName thread-handle) "]...")
    (when thread-handle (.interrupt thread-handle))
    (assoc this
      :thread-handle nil)))

#_(jdbc/insert! (get-in @titanoboa.system/systems-state [:db :system :pool])
              "titanoboa.jobs"
              {:jobid     (java.util.UUID/randomUUID)
               :jobdef    "new test"
               :revision  "123"
               :state     ":finished"
               :start     (clj-time.coerce/to-sql-time (java.util.Date.))
               :ended     (clj-time.coerce/to-sql-time (java.util.Date.))
               :stepstate ":finished"
               :stepid    "preprocessing"
               :system    ":core"
               :worker  "0"
               :job       (nippy/freeze {:lalala :a :b "asdadsda"})})

#_(-> (jdbc/query (get-in @titanoboa.system/systems-state [:db :system :pool]) "select job from titanoboa.jobs")
    first
    :job
    nippy/thaw)