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

(defn distributed-core-system [{:keys [node-id jobs-repo-path job-folder-path eviction-interval eviction-age cmd-exchange-name jobs-cmd-exchange-name mq-host mq-port mq-username mq-password mq-vhost
                                       dont-log-properties trim-logged-properties properties-trim-size] :as config}]
  (component/system-map
    :server-config config
    :node-id (:node-id config)
    :job-state (agent {})
    :eviction-list (agent {})
    :eviction-worker (component/using (cache/map->CacheEvictionComponent {:eviction-interval (or eviction-interval 10000)
                                                                          :eviction-age (or eviction-age  15000)})
                                      {:eviction-agent :eviction-list
                                       :job-cache-agent :job-state})
    :dont-log-properties (boolean dont-log-properties)
    :trim-logged-properties (boolean trim-logged-properties)
    :properties-trim-size (or properties-trim-size 100)
    :job-defs (atom (titanoboa.repo/get-all-revisions! jobs-repo-path))
    :repo-watcher (component/using (titanoboa.repo/map->RepoWatcherComponent {:folder-path jobs-repo-path})
                                   {:jd-atom :job-defs})
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
    :cmd-exchange-name cmd-exchange-name
    :jobs-cmd-exchange-name jobs-cmd-exchange-name
    :cmd-exchange-chan (component/using (channel/->RMQExchangeBroadcast (:cmd-exchange-name config) "10000" {"host" (:node-id config)})
                                   {})
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

(defn distributed-worker-system [{:keys [mq-host mq-port mq-username mq-password mq-vhost dont-log-properties trim-logged-properties  eviction-list
                                         properties-trim-size jobs-cmd-exchange jobs-cmd-exchange-name node-id sys-key worker-id restart-workers-on-error] :as config}]
  (component/system-map
    :server-config (:server-config config) ;;TODO passing on configuration onto workers so as they can instantiate new systems - review whether more elegant approaches exist
    :node-id node-id
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
    :cmd-exchange-chan (:cmd-exchange-chan config)
    :job-worker (component/using
                  (processor/map->JobWorker {:eviction-agent eviction-list :jobs-cmd-exchange jobs-cmd-exchange :sys-key sys-key :worker-id worker-id :restart-workers-on-error restart-workers-on-error
                                             :dont-log-properties dont-log-properties :trim-logged-properties trim-logged-properties :properties-trim-size properties-trim-size})
                  {:node-id :node-id
                   :in-jobs-ch :jobs-chan
                   :new-jobs-ch :new-jobs-chan
                   :out-jobs-ch :jobs-chan
                   :finished-ch :finished-jobs-chan
                   :state-agent :job-state
                   :mq-session :mq-session
                   :cmd-exchange-ch :cmd-exchange-chan ;;TODO passing on cmd chan onto workers so as they can instantiate new systems - review whether more elegant approaches exist
                   :suspended-ch :suspended-jobs-chan
                   :jobs-cmd-exchange :jobs-cmd-exchange
                   :server-config :server-config})))

(defn cluster-broadcast-system [{:keys [state-fn node-id mq-host mq-port mq-username mq-password mq-vhost heartbeat-exchange-name] :as config}]
  (component/system-map
    :mq-connection (channel/->RMQConnectionComponent {:host (or mq-host "localhost")
                                                      :port (or mq-port 5672)
                                                      :username (or mq-username "guest")
                                                      :password (or mq-password "guest")
                                                      :vhost (or mq-vhost "/")
                                                      :connection-name "titanoboa-cluster-broadcast"}
                                                     nil)
    :mq-session (component/using
                  (channel/map->RMQSessionComponent {})
                  {:connection-comp  :mq-connection})
    :broadcast (component/using
                 (channel/map->SystemStateBroadcast {:exchange-name (or heartbeat-exchange-name "heartbeat")
                                                   :node-id node-id ;;(.getHostAddress (java.net.InetAddress/getLocalHost))
                                                   :state-fn state-fn
                                                   :broadcast-interval 5000
                                                   :msg-exipre "5000"})
                 {:session-comp :mq-session})))

(defn cluster-subscription-system [{:keys [processing-fn node-id exchange-name mq-host mq-port mq-username mq-password mq-vhost] :as config}]
  (component/system-map
    :mq-connection (channel/->RMQConnectionComponent {:host (or mq-host "localhost")
                                                      :port (or mq-port 5672)
                                                      :username (or mq-username "guest")
                                                      :password (or mq-password "guest")
                                                      :vhost (or mq-vhost "/")
                                                      :connection-name "titanoboa-cluster-subscription"}
                                                     nil)
    :mq-session (component/using
                  (channel/map->RMQSessionComponent {})
                  {:connection-comp :mq-connection})
    :subs-ch (async/chan (async/sliding-buffer 64))
    :exchange-name exchange-name
    :exchange (component/using (channel/map->ExchangeComponent {})
                                         {:session-comp :mq-session
                                          :exchange-name :exchange-name})
    :subscription (component/using
                    (channel/map->ExchangeSubscription {:node-id node-id})
                    {:session-comp :mq-session
                     :exchange-comp :exchange
                     :subs-ch :subs-ch})
    :processor (component/using
                 (map->ExchangeSubsProcessor {:processing-fn processing-fn})
                 {:subs-ch :subs-ch})))