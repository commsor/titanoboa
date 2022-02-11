; Copyright (c) Commsor Inc. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.system
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [titanoboa.util :as util]
            [titanoboa.channel :as channel]))

(def lock (Object.))
(def n-cpu (.availableProcessors (Runtime/getRuntime)))

;;TODO maybe handle state directly in the components? this would be an additon to components default functionality...
#_(def systems-state {:core {:system nil
                             :state :running ;;starting/error
                             :workers []
                             :lock (Object.)}})
;;TODO decide whether catalogue will be always just passed as parameter or stored centraly!
#_(def systems-catalogue (atom {}))
#_(defn set-system-catalogue! [sc]
  (reset! systems-catalogue sc))

(def systems-state (atom {}))

(defn scope->key [sys-key scope]
  (if scope (keyword scope (name sys-key)) sys-key))

(defn has-worker? [sys-key systems-catalogue]
  (contains? (sys-key systems-catalogue) :worker-def))

(defn has-active-workers? [sys-key state-atom]
  (and (contains? (sys-key @state-atom) :workers)
       (not-every? nil? (get-in @state-atom [sys-key :workers]))))

(defn is-running?
  ([ns-sys-key state-atom]
   (and (get @state-atom ns-sys-key) (= (get-in @state-atom [ns-sys-key :state]) :running)))
  ([ns-sys-key]
   (is-running? ns-sys-key systems-state)))

(defn sys->scope [system scope]
  "Tweaks queue names (for :jobs-chan and :new-jobs-queue) in the system if scope is provided."
  (if scope (-> system
                (update-in  [:jobs-chan :queue] str "-" scope)
                (update-in  [:new-jobs-chan :queue] str "-" scope)
                (update-in  [:finished-jobs-chan :queue] str "-" scope))
            system))

(defn conf->scope [config scope]
  "Tweaks queue names (for :jobs-chan and :new-jobs-queue) in the system configuration if scope is provided."
  (if scope (-> config
                (update-in  [:jobs-queue] str "-" scope)
                (update-in  [:new-jobs-queue] str "-" scope)
                (update-in  [:finished-jobs-queue] str "-" scope)) ;;TODO decide whether finished queue should be system specific (likely yes) - depends on how job archival will be sorted out
            config))

(defn prepare-config [config sys-key]
  (let [stripped-conf (dissoc config :systems-config)
        merged-conf (merge stripped-conf (get-in config [:systems-config sys-key]))]
    merged-conf))

(defn start-system! [sys-key systems-catalogue config & [scope]]
  "Starts the system defined under the given key in systems bundles catalogue and registers it in provided systems-state.
  If already present in state atom it is assumed to be already running, no action is taken and false is returned."
  (locking lock
    (let [ns-sys-key (scope->key sys-key scope)]
      (if-not (get @systems-state ns-sys-key)
        (let [{:keys [system-def worker-def]} (get systems-catalogue sys-key)];;TODO add error handling and corresponding state :error
          (log/info "Starting system" sys-key "...")
          (if worker-def
            (swap! systems-state assoc ns-sys-key {:system nil :state :starting :workers []})
            (swap! systems-state assoc ns-sys-key {:system nil :state :starting}))
          (try
            (let [live-system (-> config ;;(component/start (system-def (conf->scope (prepare-config config sys-key) scope)))
                                  (prepare-config sys-key)
                                  (conf->scope scope)
                                  system-def
                                  component/start)]
              (swap! systems-state update-in [ns-sys-key] assoc :system live-system :state :running)
              (log/info "System" sys-key "started"))
            (catch Exception e
              (log/error e "Error starting system" sys-key "!")
              (swap! systems-state update-in [ns-sys-key] assoc :system nil :state :error :stack-trace (.getStackTrace e))))
          true)
        false))))

(defn start-workers! [sys-key systems-catalogue & [w-cnt scope]]
  (locking lock
    (let [ns-sys-key (scope->key sys-key scope)]
      (if (is-running? ns-sys-key systems-state)
        (let [{:keys [worker-def worker-count]} (get systems-catalogue sys-key)
              system (get-in @systems-state [ns-sys-key :system])
              n (or  w-cnt worker-count n-cpu)]
          (log/info "Starting" n "workers for system" ns-sys-key ":")
          (doseq [x (range n)]
            (do
             (log/info "Starting a worker for system" ns-sys-key "...")
             (swap! systems-state update-in [ns-sys-key :workers] conj (component/start (worker-def (assoc system :worker-id x :sys-key sys-key)))))))
        (throw (IllegalStateException. "Workers cannot be started as the System is not running!"))))))

;;TODO add error handling? Or leave the state handling to systems?
(defn stop-worker! [ns-sys-key idx]
  "Stops nth worker from the worker pool."
  (locking lock
    (component/stop (get-in @systems-state [ns-sys-key :workers idx]))
    (swap! systems-state assoc-in [ns-sys-key :workers idx] nil)))

(defn stop-all-workers! [ns-sys-key]
  (locking lock
    (doall (map component/stop (get-in @systems-state [ns-sys-key :workers])))
    (swap! systems-state assoc-in [ns-sys-key :workers] [])))

(defn cleanup-system! [system]
  "This function is meant for removal of resources allocated by a system that were not released by the system's stop function.
  E.g. a queue that was created by a system but could not be deleted upon systems stopping since it still might be
  used by other system's instances on other nodes.
  Cleanup should be performed only when 100% certain that the system has been stopped across the entire cluster!"
  (doall (map (fn [[k v]] (when (satisfies? channel/Cleanable v) (.cleanup! v))) system)))

(defn stop-system! [sys-key & [scope]]
  "Stops system based on provided system key (and scope).
  Returns the stopped system object (com.stuartsierra.component.SystemMap)."
  (locking lock
    (let [ns-sys-key (scope->key sys-key scope)]
      (if-not (is-running? ns-sys-key systems-state)
        (throw (IllegalStateException. "System cannot be stopped as it is not running!")))
      (when (has-active-workers? ns-sys-key systems-state)
        (log/info "Stopping all workers for system" ns-sys-key)
        (stop-all-workers! ns-sys-key))
      (log/info "Stopping system" ns-sys-key "...")
      (swap! systems-state update-in [ns-sys-key] assoc :state :stopping)
      (let [stopped-system (component/stop (get-in @systems-state [ns-sys-key :system]))]
        (swap! systems-state dissoc ns-sys-key)
        stopped-system))));;TODO add error handling

(defn restart-system! [sys-key systems-catalogue config & [scope]]
  (locking lock
    (if scope
      (do
        (stop-system! sys-key scope)
        (start-system! sys-key systems-catalogue config scope))
      (do
        (stop-system! sys-key)
        (start-system! sys-key systems-catalogue config)))))

(defn run-systems-onstartup! [systems-catalogue config &[n]]
  "runs all system bundles' primary systems whose scope is node-wide (and not job local)"
  (log/info "Starting systems at startup...")
  (doall
    (map (fn [[sys-key {:keys [autostart]}]]
          (if autostart
            (do
             (start-system! sys-key systems-catalogue config)
             (if (has-worker? sys-key systems-catalogue)
               (start-workers! sys-key systems-catalogue)))))
       systems-catalogue)))

(defn stop-all-systems! []
  (locking lock
    (mapv (fn [[sys-key {:keys [state]}]]
            (when (= state :running)
              (stop-system! sys-key)))
      @systems-state)))

(defn start-system-bundle! [sys-key systems-catalogue config &[n scope]]
  "starts system and specified number of workers"
  (start-system! sys-key systems-catalogue config scope)
  (when (has-worker? sys-key systems-catalogue)
    (start-workers! sys-key systems-catalogue n scope)))


(defn live-systems []
  (let [systems (mapv (fn [[sys-key {:keys [state workers]}]]
                        [sys-key (merge {:state state}
                                        (if workers {:workers (vec (map-indexed (fn [idx itm] (if itm idx nil)) workers))}))])
                      @systems-state)]
    systems))