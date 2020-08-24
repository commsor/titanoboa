; Copyright (c) Miroslav Kubicek. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.system.local
  (:require [clojure.core.async :as async :refer [go-loop go >! <! >!! <!! chan]]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [titanoboa.channel :as channel]
            [titanoboa.actions :as actions]
            [titanoboa.processor :as processor]
            [titanoboa.database :as db]
            [titanoboa.cache :as cache]
            [titanoboa.repo :as repo]))

(defn local-core-system [{:keys [node-id jobs-repo-path job-folder-path  new-jobs-chan jobs-chan finished-jobs-chan eviction-interval eviction-age] :as config}]
  (component/system-map
    :server-config config
    :node-id node-id
    :job-state (agent {})
    :eviction-list (agent {})
    :eviction-worker (component/using (cache/map->CacheEvictionComponent {:eviction-interval (or eviction-interval (* 1000 60 15))
                                                                          :eviction-age (or eviction-age (* 1000 60 180))})
                                      {:eviction-agent :eviction-list
                                       :job-cache-agent :job-state})
    :job-defs (atom (titanoboa.repo/get-all-revisions! jobs-repo-path))
    :repo-watcher (component/using (titanoboa.repo/map->RepoWatcherComponent {:folder-path jobs-repo-path})
                                   {:jd-atom :job-defs})
    :job-folder-root job-folder-path
    :action-chan (chan 32)
    :action-processor (component/using
                        (actions/map->ActionProcessorComponent {:threadpool-size 4})
                        {:action-requests-ch :action-chan})
    :new-jobs-chan new-jobs-chan
    :jobs-chan jobs-chan
    :finished-jobs-chan finished-jobs-chan))


(defn local-worker-system [config]
  (component/system-map
    :server-config (:server-config config) ;;TODO passing on configuration onto workers so as they can instantiate new systems - review whether more elegant approaches exist
    :node-id (:node-id config)
    :job-state (:job-state config)
    :eviction-list (:eviction-list config)
    :new-jobs-chan (:new-jobs-chan config)
    :jobs-chan (:jobs-chan config)
    :finished-jobs-chan (:finished-jobs-chan config)
    :job-state (:job-state config)
    :job-worker (component/using
                  (processor/map->JobWorker {})
                  {:node-id :node-id
                   :in-jobs-ch :jobs-chan
                   :new-jobs-ch :new-jobs-chan
                   :out-jobs-ch :jobs-chan
                   :finished-ch :finished-jobs-chan
                   :state-agent :job-state
                   :eviction-agent :eviction-list
                   :server-config :server-config})))

(defn archival-system [config]
  (component/system-map
    :finished-jobs-chan (:finished-jobs-chan config)
    :db-pool (db/map->JdbcPoolComponent {:config (merge
                                                   {:jdbc-url "jdbc:postgresql://localhost:5432/mydb?currentSchema=titanoboa"
                                                    :user "postgres"
                                                    :password "postgres"
                                                    :driver-class "org.postgresql.Driver"
                                                    :minimum-pool-size 2
                                                    :maximum-pool-size 15
                                                    :excess-timeout (* 30 60)
                                                    :idle-timeout (* 3 60 60)}
                                                   config)})
    :archive-worker (component/using (db/map->JobArchivingComponent {})
                                     {:ds :db-pool
                                      :finished-jobs-chan :finished-jobs-chan})))
