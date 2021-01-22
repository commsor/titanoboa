 (ns rmq-system-test
   (:use clojure.test)
   (:require [me.raynes.fs :as fs]
             [titanoboa.exp :as exp])
   (:import (io.titanoboa.java SampleWorkloadImpl)))

(defn hello-fn [p]
  {:message (str "Hello " (or (:name p) "human") "!")
   :return-code (nil? (:name p))})

(defn greet-fn [p]
  {:message (str (:message p) " Nice to meet you!")})

(defn fill-in-blanks [p]
  {:message (str (:message p) " What is your name?")})

(def job-def-flow {:first-step "step1"
                   :name            "test"
                   :type            nil
                   :properties      {:name nil}
                   :steps           [{:id          "step1" :type :custom :supertype :tasklet :next [[false "step2"] [true "step3"]]
                                      :workload-fn 'local-system-test/hello-fn
                                      :properties  {}}
                                     {:id          "step2" :type :custom :supertype :tasklet
                                      :workload-fn 'local-system-test/greet-fn
                                      :next        []
                                      :properties  {}}
                                     {:id          "step3" :type :custom :supertype :tasklet
                                      :workload-fn 'local-system-test/fill-in-blanks
                                      :next        []
                                      :properties  {}}]})

(def job-def-java {:first-step "step1"
                   :name       "java-test"
                   :type       nil
                   :properties {"name" nil}
                   :steps      [{:id          "step1" :type :custom :supertype :tasklet :next []
                                 :workload-fn 'io.titanoboa.java.SampleWorkloadImpl
                                 :properties  {}}]})

(def job-def-java-exp {:first-step "step1"
                       :name       "java-test2"
                       :type       nil
                       :properties {"name" nil}
                       :steps      [{:id          "step1" :type :custom :supertype :tasklet :next [["*" "step2"]]
                                     :workload-fn (exp/->Expression "io.titanoboa.java.SampleWorkloadImpl" "clojure")
                                     :properties {}}
                                    {:id          "step2" :type :custom :supertype :tasklet :next []
                                     :workload-fn (exp/map->Expression{:value " p -> {\n \t\tString greeting = (String) p.get(\"greeting\");\n   \t\tgreeting = greeting + \" Nice to meet you!\";\n \t\tjava.util.HashMap propertiesToReturn = new java.util.HashMap ();\n   \t\tpropertiesToReturn.put (\"greeting\" , greeting);\n   \t\treturn propertiesToReturn;\n }", :type "java"})
                                     :properties {}}]})

(deftest ^:rmq start-system!
  (require 'titanoboa.channel.rmq)
  (require 'titanoboa.system.rabbitmq)
  (titanoboa.system/start-system! :core-rmq
                                  {:core-rmq {:system-def (resolve 'titanoboa.system.rabbitmq/distributed-core-system)
                                          :worker-def (resolve 'titanoboa.system.rabbitmq/distributed-worker-system)
                                          :autostart  true
                                          :worker-count 2}}
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
                                           :worker-count 2}}))
(deftest ^:rmq run-job-test-flow
  (let [{:keys [properties state]}(titanoboa.processor/run-job! :core-rmq
                                                                {:jobdef     job-def-flow
                                                                 :properties {:name "World"}}
                                                                true)]
    (is (= state :finished))
    (is (= (:message properties) "Hello World! Nice to meet you!")))
  (let [{:keys [properties state]}(titanoboa.processor/run-job! :core-rmq
                                                                {:jobdef     job-def-flow
                                                                 :properties {:name nil}}
                                                                true)]
    (is (= state :finished))
    (is (= (:message properties) "Hello human! What is your name?"))))

(deftest ^:rmq run-job-test-java
  (let [{:keys [properties state]}(titanoboa.processor/run-job! :core-rmq
                                                                {:jobdef     job-def-java
                                                                 :properties {"name" "World"}}
                                                                true)]
    (is (= state :finished))
    (is (= (get properties "greeting") "Hello World!")))
  (let [{:keys [properties state]}(titanoboa.processor/run-job! :core-rmq
                                                                {:jobdef     job-def-java-exp
                                                                 :properties {"name" "World"}}
                                                                true)]
    (is (= state :finished))
    (is (= (get properties "greeting") "Hello World! Nice to meet you!"))))

(deftest ^:rmq stop-system!
  (titanoboa.system/stop-all-systems!))

(defn test-ns-hook
  "Run tests in a sorted order."
  []
  (start-system!)
  (start-workers!)
  (run-job-test-flow)
  (run-job-test-java)
  (stop-system!))
