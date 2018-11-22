(ns local-system-test
  (:use clojure.test)
  (:require [me.raynes.fs :as fs]))

(def new-jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024)))
(def jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024)))
(def finished-jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024)))

(defn hello-fn [p]
  {:message (str "Hello " (or (:name p) "human") "!")
   :return-code (nil? (:name p))})

(defn greet-fn [p]
  {:message (str (:message p) " Nice to meet you!")})

(defn fill-in-blanks [p]
  {:message (str (:message p) " What is your name?")})

(def job-def {:first-step "step1"
              :name       "test"
              :type       nil
              :properties {:name nil}
              :steps      [{:id          "step1" :type :custom :supertype :tasklet :next [[false "step2"] [true "step3"]]
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

(deftest start-system!
  (titanoboa.system/start-system! :core-local
                                  {:core-local {:system-def   #'titanoboa.system.local/local-core-system
                                          :worker-def   #'titanoboa.system.local/local-worker-system
                                          :autostart    true
                                          :worker-count 2}}
                                  {:new-jobs-chan      new-jobs-chan
                                   :jobs-chan          jobs-chan
                                   :finished-jobs-chan finished-jobs-chan
                                   :node-id            "localhost"
                                   :eviction-interval  (* 1000 60 5)
                                   :eviction-age       (* 1000 60 10)
                                   :jobs-repo-path     "dev-resources/repo-test/"
                                   :job-folder-path    "dev-resources/job-folders/"}))

(deftest start-workers!
  (titanoboa.system/start-workers! :core-local
                                   {:core-local {:system-def   #'titanoboa.system.local/local-core-system
                                           :worker-def   #'titanoboa.system.local/local-worker-system
                                           :autostart    true
                                           :worker-count 2}}))
(deftest run-job
  (let [{:keys [properties state]}(titanoboa.processor/run-job! :core-local
                              {:jobdef job-def
                               :properties {:name "World"}}
                              true)]
    (is (= state :finished))
    (is (= (:message properties) "Hello World! Nice to meet you!")))
  (let [{:keys [properties state]}(titanoboa.processor/run-job! :core-local
                                                                {:jobdef job-def
                                                                 :properties {:name nil}}
                                                                true)]
    (is (= state :finished))
    (is (= (:message properties) "Hello human! What is your name?"))))

(deftest stop-system!
  (titanoboa.system/stop-all-systems!))

(defn test-ns-hook
  "Run tests in a sorted order."
  []
  (start-system!)
  (start-workers!)
  (run-job)
  (stop-system!))