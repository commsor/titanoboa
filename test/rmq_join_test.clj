 ; Copyright (c) Miroslav Kubicek. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns rmq-join-test
   (:use clojure.test)
   (:require [titanoboa.exp :as exp]
             [titanoboa.system.local]))

(def new-jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024)))
(def jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024)))
(def finished-jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024)))

(def steps (atom []))

(defn fib-nth
  ([n id]
  (if (< n 2)
    (swap! steps conj {:id (str "Fib " n " " id)
                       :type :custom
                       :supertype :tasklet
                       :properties {:a n}
                       :workload-fn (titanoboa.exp/map->Expression {:value "identity" :type "clojure"})
                       :next [["*" (str "Join " (pop id))]]})
    (do
      (swap! steps conj {:id (str "Fib " n " " id)
                         :allow-parallel? true
                         :type :custom
                         :supertype :tasklet
                         :properties {:a n}
                         :workload-fn (titanoboa.exp/map->Expression {:value "(constantly {})" :type "clojure"})
                         :next [["*" (str "Fib " (- n 1) " " (conj id (- n 1)))]
                                ["*" (str "Fib " (- n 2) " " (conj id (- n 2)))]]})
      (fib-nth (- n 1) (conj id (- n 1)))
      (fib-nth (- n 2) (conj id (- n 2)))
      (swap! steps conj {:id (str "Join " id)
                         :type :custom-join
                         :supertype :join
                         :properties {:merge-with-fn (titanoboa.exp/map->Expression {:value "+" :type "clojure"})}
                         :workload-fn (titanoboa.exp/map->Expression {:value "(constantly {})" :type "clojure"})
                         :next (if-not (empty? id)
                                 [["*" (str "Join " (pop id))]]
                                 [])}))))
  ([n]
   (fib-nth n [])))

(fib-nth 5)

(def job-def-flow {:first-step "Fib 5 []"
                   :name            "Fibonacci5"
                   :type            nil
                   :properties      {:name nil}
                   :steps           @steps})


(deftest ^:rmq start-system!
  (require 'titanoboa.channel.rmq)
  (require 'titanoboa.system.rabbitmq)
  (titanoboa.system/start-system! :core-rmq
                                  {:core-rmq {:system-def (resolve 'titanoboa.system.rabbitmq/distributed-core-system)
                                              :worker-def (resolve 'titanoboa.system.rabbitmq/distributed-worker-system)
                                              :autostart  true
                                              :worker-count 8}}
                                  {:new-jobs-queue "titanoboa-bus-queue-0"
                                   :jobs-queue "titanoboa-bus-queue-1"
                                   :archival-queue "titanoboa-bus-archive"
                                   :heartbeat-exchange-name "heartbeat"
                                   :cmd-exchange-name "command"
                                   :jobs-cmd-exchange-name "jobs-command"
                                   :enable-cluster false
                                   :node-id            "localhost"
                                   :eviction-interval  (* 1000 60 5)
                                   :eviction-age       (* 1000 60 10)
                                   :jobs-repo-path     "dev-resources/repo-test/"
                                   :job-folder-path    "dev-resources/job-folders/"}))

(deftest ^:rmq start-workers!
  (titanoboa.system/start-workers! :core-rmq
                                   {:core-rmq {:system-def (resolve 'titanoboa.system.rabbitmq/distributed-core-system)
                                               :worker-def (resolve 'titanoboa.system.rabbitmq/distributed-worker-system)
                                               :autostart  true
                                               :worker-count 6}}))
(deftest ^:rmq run-job-test-joins
  (let [{:keys [properties state]}(titanoboa.processor/run-job! :core-rmq
                                                                {:jobdef     job-def-flow
                                                                 :properties {}}
                                                                true)]
    (is (= state :finished))
    (is (= (:a properties) 5))))

(deftest ^:rmq stop-system!
  (titanoboa.system/stop-all-systems!))

(defn test-ns-hook
  "Run tests in a sorted order."
  []
  (start-system!)
  (start-workers!)
  (run-job-test-joins)
  (stop-system!))

