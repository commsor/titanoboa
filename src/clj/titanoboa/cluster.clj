; Copyright (c) Miroslav Kubicek. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.cluster
  (:require [titanoboa.system :as system]
            [titanoboa.api :as api]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [titanoboa.util :as util]))

(def lock (Object.))

(def node-id nil) ;;(.getHostAddress (java.net.InetAddress/getLocalHost))
(def cluster-enabled false)

(def *cluster-aware* false)

(def  *cmd-subs-active* false)

(defn cluster-enabled? [] cluster-enabled)

;; {"node-id" {:systems {:core {:state :running, :workers [0 1 2 3 4 5 6 7]}} :timestamp timestamp}}
(def cluster-state-sys (atom {}))

(def cluster-state-jobs (atom {}))

(def cluster-broadcast nil)

(def cluster-state-subscription nil)

(def cluster-cmd-subscription nil)

(defn process-cluster-command [c]
  (eval c))

(defn get-host []
  (-> node-id
    (clojure.string/split #":")
    (get 0)))

(defn merge-job-states [state-a state-b]
  "Merges two system jobs states in a format of {:system {'job-id' job-map}}.
  If two same job ids appear, the job with longer history log is assumed to be newer and s used to override the older one."
  (merge-with (fn [sys1 sys2]
                (merge-with (fn [job1 job2] ;;TODO also check for timestamp and/or steps count
                              (if (> (count (:history job1)) (count (:history job2)))
                                (do
                                  (log/debug "Overriding job with step/next-step [" (:step job2) " / " (:next-step job2) "] with job with step/next-step [" (:step job1) " / " (:next-step job1) "]")
                                  job1)
                                (do (log/debug "Overriding job with step/next-step [" (:step job1) " / " (:next-step job1) "] with job with step/next-step [" (:step job2) " / " (:next-step job2) "]")
                                    job2)))
                            sys1 sys2))
              state-a state-b))

(defn process-broadcast [m]
  (let [kv-pair (first m)
        node-id (first kv-pair)
        jobs (:jobs (second kv-pair))
        node-properties (dissoc (second kv-pair) :jobs)]
    (log/debug "Processing cluster broadcast " m " with metadata [" (meta m) "]")
    (swap! cluster-state-sys assoc node-id node-properties)
    (swap! cluster-state-jobs merge-job-states jobs)))

(defn get-sys-load []
  {:system-cpu-load (.getSystemCpuLoad (java.lang.management.ManagementFactory/getOperatingSystemMXBean))
   :process-cpu-load (.getProcessCpuLoad (java.lang.management.ManagementFactory/getOperatingSystemMXBean))
   :max-memory (.maxMemory (Runtime/getRuntime))
   :allocated-memory (.totalMemory (Runtime/getRuntime))
   :free-memory (.freeMemory (Runtime/getRuntime))})

(defn init-cluster! [{:keys [cluster-state-broadcast cluster-state-subs cluster-state-fn heartbeat-exchange-name cluster-cmd-subs cluster-cmd-fn cmd-exchange-name] :as server-config}]
  (alter-var-root #'node-id (constantly (:node-id server-config)))
  (when (:enable-cluster server-config)
    (alter-var-root #'cluster-enabled (constantly true))
    (alter-var-root #'cluster-broadcast
                    (constantly ;;TODO why not include following systems in the regular system catalogue?
                      (cluster-state-broadcast (merge server-config
                                                      {:state-fn (fn [] (merge (get-sys-load)
                                                                        {:systems   (merge-with merge (into {} (system/live-systems)) (:systems-catalogue server-config))
                                                                         :jobs      (api/get-jobs-states)
                                                                         :timestamp (java.util.Date.)}))}))))
    (alter-var-root #'cluster-state-subscription
                    (constantly
                      (cluster-state-subs (merge server-config
                                                 {:exchange-name heartbeat-exchange-name
                                                  :processing-fn cluster-state-fn}))))
    (alter-var-root #'cluster-cmd-subscription
                    (constantly
                      (cluster-cmd-subs (merge server-config
                                                 {:exchange-name cmd-exchange-name
                                                  :processing-fn cluster-cmd-fn}))))))

(defn start-broadcast! []
  (if (cluster-enabled?)
    (alter-var-root #'cluster-broadcast component/start)
    #_(throw (IllegalStateException. "Cannot start heartbeat broadcast since clustering is not enabled!"))))

(defn stop-broadcast! []
  (if (cluster-enabled?)
    (alter-var-root #'cluster-broadcast component/stop)
    #_(throw (IllegalStateException. "Cannot start heartbeat broadcast since clustering is not enabled!"))))

(defn start-state-subs! []
  (when-not (cluster-enabled?) (throw (IllegalStateException. "Cannot monitor cluster nodes' heartbeat since clustering is not enabled!")))
  (locking lock
    (when-not *cluster-aware*
      (alter-var-root #'cluster-state-subscription component/start)
      (alter-var-root #'*cluster-aware* not))))

(defn stop-state-subs! []
  (when-not (cluster-enabled?) (throw (IllegalStateException. "Cannot monitor cluster nodes' heartbeat since clustering is not enabled!")))
  (locking lock
    (when *cluster-aware*
      (alter-var-root #'cluster-state-subscription component/stop)
      (alter-var-root #'*cluster-aware* not))))

(defn start-cmd-subs! []
  (when-not (cluster-enabled?) (throw (IllegalStateException. "Cannot subscribe to command exchange since clustering is not enabled!")))
  (locking lock
    (when-not *cmd-subs-active*
      (alter-var-root #'cluster-cmd-subscription component/start)
      (alter-var-root #'*cmd-subs-active* not))))

(defn stop-cmd-subs! []
  (when-not (cluster-enabled?) (throw (IllegalStateException. "Cannot subscribe to command exchange since clustering is not enabled!")))
  (locking lock
    (when *cmd-subs-active*
      (alter-var-root #'cluster-cmd-subscription component/stop)
      (alter-var-root #'*cmd-subs-active* not))))


(defn state-of-all-nodes [this-nodes-sysmap] ;;TODO make timeouts configurable
  (if (and (cluster-enabled?) *cluster-aware*)
    (reduce-kv
      (fn [m k v]
        (let [delta (- (.getTime (java.util.Date.)) (.getTime (:timestamp v)))]
        (assoc m k (assoc v :last-hearbeat-age delta
                            :state (cond
                                     (< delta 60000) :live
                                     (< delta 120000) :non-responsive
                                     :else :down)))))
      {node-id
       (merge {:systems this-nodes-sysmap :last-hearbeat-age 0 :source true :state :live} (get-sys-load))}
      @cluster-state-sys)
    (throw (IllegalStateException. "Cluster is not enabled and/or this node is not aware of other nodes."))))

(defn evict-old-jobs [jobs-map eviction-age cur-t]
  (->> jobs-map
       (filter (fn [[k v]] (or (not (:end v))
                          (< cur-t
                             (+ (.getTime (:end v)) eviction-age)))))
       (into {})))

(defrecord JobsCleanupComponent [thread-handle eviction-interval eviction-age]
  component/Lifecycle
  (start [this]
    (log/info "Starting JobsCleanupComponent...")
    (if thread-handle
      this
      (let [th (Thread. (fn[]
                          (log/info "Starting JobsCleanupComponent thread [" (.getName (Thread/currentThread)) "].")
                          (loop [t (java.util.Date.)]
                            (swap! cluster-state-jobs util/update-in-* [*] evict-old-jobs eviction-age (.getTime t))
                            (Thread/sleep eviction-interval)
                            (recur (java.util.Date.))))
                          (str "JobsCleanupComponent thread " (rand-int 9)))]
        (.start th)
        (assoc this
          :thread-handle th))))
  (stop [this]
    (log/info "Stopping JobsCleanupComponent thread [" (.getName thread-handle) "]...")
    (if thread-handle (.interrupt thread-handle))
    (assoc this
      :thread-handle nil)))