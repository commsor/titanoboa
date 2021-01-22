;   Copyright (c) Miroslav Kubicek. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
;   which can be found in the LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns titanoboa.system.rabbitmq
  (:require [clojure.core.async :as async :refer [go-loop go >! <! >!! <!! chan]]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [titanoboa.channel :refer [map->ExchangeSubsProcessor]]
            [titanoboa.channel.rmq :as channel]
            [titanoboa.actions :as actions]
            [titanoboa.processor :as processor]
            [titanoboa.repo]
            [titanoboa.database :as db]
            [titanoboa.cache :as cache]))

(defn distributed-core-system [{:keys [node-id jobs-repo-path job-folder-path eviction-interval eviction-age cmd-exchange-name jobs-cmd-exchange-name mq-host mq-port mq-username mq-password mq-vhost] :as config}]
  (component/system-map
    :server-config config
    :node-id (:node-id config)
    :job-state (agent {})
    :eviction-list (agent {})
    :eviction-worker (component/using (cache/map->CacheEvictionComponent {:eviction-interval (or eviction-interval 10000)
                                                                          :eviction-age (or eviction-age  15000)})
                                      {:eviction-agent :eviction-list
                                       :job-cache-agent :job-state})
    :job-defs (atom (titanoboa.repo/get-all-revisions! jobs-repo-path))
    :job-folder-root job-folder-path
    :action-chan (chan 32)
    :action-processor (component/using
                        (actions/map->ActionProcessorComponent {:threadpool-size 8})
                        {:action-requests-ch :action-chan})
    :mq-connection (channel/->RMQConnectionComponent {:host (or mq-host "localhost")
                                                      :port (or mq-port 5672)
                                                      :username (or mq-username "guest")
                                                      :password (or mq-password "guest")
                                                      :vhost (or mq-vhost "/")
                                                      :connection-name "titanoboa-connection"}
                                                     nil)
    :mq-session-pool (component/using
                       (channel/map->RMQSessionPoolComponent {:pool-size 6})
                       {:connection-comp :mq-connection})
    :jobs-cmd-exchange-name jobs-cmd-exchange-name
    :jobs-cmd-exchange (component/using (channel/map->RMQExchange {:exchange jobs-cmd-exchange-name :type "topic" :routing-key-prefix "job.id." :routing-key :jobid})
                                        {:sessionpool-comp :mq-session-pool})
    :new-jobs-queue (:new-jobs-queue config)
    :jobs-queue (:jobs-queue config)
    :archival-queue (:archival-queue config)
    :new-jobs-q-construct (component/using (channel/map->QueueComponent {})
                                     {:sessionpool-comp :mq-session-pool
                                      :queue-name :new-jobs-queue})
    :jobs-q-construct (component/using (channel/map->QueueComponent {})
                                 {:sessionpool-comp :mq-session-pool
                                  :queue-name :jobs-queue})
    :archive-q-construct (component/using (channel/map->QueueComponent {})
                                       {:sessionpool-comp :mq-session-pool
                                        :queue-name :archival-queue})
    :new-jobs-chan (channel/rmq-chan (:new-jobs-queue config) false 20)
    :jobs-chan (channel/rmq-chan (:jobs-queue config) false 20)
    :suspended-jobs-chan (channel/rmq-chan (:archival-queue config) false 20)
    :finished-jobs-chan (channel/rmq-chan (:archival-queue config) false 20)))

(defn archival-system [{:keys [mq-host mq-port mq-username mq-password mq-vhost] :as config}]
  (component/system-map
    :archival-queue (:archival-queue config)
    :archive-q-construct (component/using (channel/map->QueueComponent {})
                                          {:session-comp :mq-session
                                           :queue-name :archival-queue})
    :finished-jobs-chan (channel/rmq-chan (:archival-queue config) false 20)
    :mq-connection (channel/->RMQConnectionComponent {:host              (or mq-host "localhost")
                                                      :port              (or mq-port 5672)
                                                      :username          (or mq-username "guest")
                                                      :password          (or mq-password "guest")
                                                      :vhost             (or mq-vhost "/")
                                                      :connection-name   "archival-connection"}
                                                     nil)
    :mq-session (component/using
                           (channel/map->RMQSessionComponent {})
                           {:connection-comp  :mq-connection})
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
                                      :mq-session-comp :mq-session
                                      :finished-jobs-chan :finished-jobs-chan
                                      :_q-construct :archive-q-construct})))

(defn distributed-worker-system [{:keys [mq-host mq-port mq-username mq-password mq-vhost dont-log-properties trim-logged-properties properties-trim-size jobs-cmd-exchange jobs-cmd-exchange-name node-id sys-key worker-id restart-workers-on-error] :as config}]
  (component/system-map
    :server-config (:server-config config) ;;TODO passing on configuration onto workers so as they can instantiate new systems - review whether more elegant approaches exist
    :node-id node-id
    :dont-log-properties (boolean dont-log-properties)
    :trim-logged-properties (boolean trim-logged-properties)
    :properties-trim-size (or properties-trim-size 100)
    :job-state (:job-state config)
    :mq-connection (channel/->RMQConnectionComponent {:host (or mq-host "localhost")
                                                             :port (or mq-port 5672)
                                                             :username (or mq-username "guest")
                                                             :password (or mq-password "guest")
                                                             :vhost (or mq-vhost "/")
                                                             :connection-name (str "titanoboa-worker-" node-id "-" sys-key "(" worker-id ")")}
                                                            nil)
    :mq-session (component/using
                  (channel/map->RMQSessionComponent {})
                  {:connection-comp  :mq-connection})
    :new-jobs-chan (:new-jobs-chan config)
    :jobs-chan (:jobs-chan config)
    :finished-jobs-chan (:finished-jobs-chan config)
    :suspended-jobs-chan (:suspended-jobs-chan config)
    :jobs-cmd-exchange (component/using (channel/map->RMQExchange {:exchange jobs-cmd-exchange-name :type "topic" :routing-key-prefix "job.id." :routing-key :jobid})
                                        {:session-comp :mq-session})
    :eviction-list (:eviction-list config)
    :job-worker (component/using
                  (processor/map->JobWorker {})
                  {:node-id :node-id
                   :in-jobs-ch :jobs-chan
                   :new-jobs-ch :new-jobs-chan
                   :out-jobs-ch :jobs-chan
                   :finished-ch :finished-jobs-chan
                   :state-agent :job-state
                   :eviction-agent :eviction-list
                   :mq-session :mq-session
                   :suspended-ch :suspended-jobs-chan
                   :jobs-cmd-exchange :jobs-cmd-exchange
                   :server-config :server-config
                   :dont-log-properties :dont-log-properties
                   :trim-logged-properties :trim-logged-properties
                   :properties-trim-size :properties-trim-size})))
