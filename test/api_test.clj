(ns api-test
   (:use clojure.test)
  (:require [titanoboa.server :as server]
            [clj-http.client :as client :refer [request]]
            [clj-http.util :as http-util]
            [me.raynes.fs :as fs]
            [titanoboa.handler :as handler]
            [titanoboa.exp]
            [titanoboa.system.local]))

(def transit-opts
  "Transit read and write options."
  {:encode {:handlers {titanoboa.exp.Expression titanoboa.exp/transit-write-handler}}
   :decode {:handlers {"titanoboa.exp.Expression" titanoboa.exp/transit-read-handler}}})

(def transit-request-stub {:throw-exceptions false
                           :content-type :transit+json
                           :as :transit+json
                           :transit-opts transit-opts})
(def jd-name "test")
(def test-repo-path "dev-resources/repo-test/")
(def server-config {:systems-catalogue
                                     {:core  {:system-def #'titanoboa.system.local/local-core-system
                                              :worker-def #'titanoboa.system.local/local-worker-system
                                              :autostart  true
                                              :worker-count 2}}
                    :jobs-repo-path test-repo-path
                    :steps-repo-path "dev-resources/step-repo/"
                    :job-folder-path "dev-resources/job-folders/"
                    :enable-cluster false
                    :jetty {:join? false
                            :port 3000}
                    :auth? false
                    :systems-config {:core
                                     {:new-jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024))
                                      :jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024))
                                      :finished-jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024))
                                      :eviction-interval (* 1000 60 5)
                                      :eviction-age (* 1000 60 10)}}})

(def job-def {:first-step "step1"
              :name jd-name
              :type nil
              :properties {:name "World"}
              :steps [{:id "step1" :type :custom :supertype :tasklet :next [[false "step2"][true "step3"]]
                       :workload-fn (titanoboa.exp/map->Expression {:value "(fn [p] \n  {:message (str \"Hello \" (:name p)) :return-code (nil? (:name p))})", :type nil})
                       :properties {}}
                      {:id "step2" :type :custom :supertype :tasklet
                       :workload-fn #titanoboa.exp.Expression{:value "(fn [p]\n  {:message (str (:message p) \"!\")})", :type nil}
                       :next []
                       :properties {}}
                      {:id "step3" :type :custom :supertype :tasklet
                       :workload-fn #titanoboa.exp.Expression{:value "(fn [p]\n  {:message (str (:message p) \"nobody!\")})", :type nil}
                       :next []
                       :properties {}}]})

(def revision-note "loremipsum")
(def jobs-created (atom #{}))

(defn req [form-params]
  (assoc transit-request-stub :form-params form-params))

(deftest repo-not-exists
  (is (not (fs/exists? test-repo-path))))

(deftest stop-server
  (server/shutdown!)
  (is (empty (titanoboa.system/live-systems))))

(defn remove-repo []
  (fs/delete-dir test-repo-path))

(deftest start-server
  (testing "starting up titanoboa server"
    (server/start server-config)
    (is (instance? org.eclipse.jetty.server.Server server/server))
    (is (not-empty (titanoboa.system/live-systems)))))


(deftest create-job-definition
  (let [resp (client/post "http://localhost:3000/repo/jobdefinitions/test"
                          (req {:definition job-def
                                :notes      revision-note}))]
    (is (= (:status resp) 201))
    (is (number? (:body resp)))
    (is (= (:body resp) 1))))

(deftest update-job-definition
  (let [resp (client/post "http://localhost:3000/repo/jobdefinitions/test"
                          (req {:definition job-def
                                :notes      revision-note}))]
    (is (= (:status resp) 201))
    (is (number? (:body resp)))
    (is (= (:body resp) 2))))

(deftest repo-exists
  (is (fs/exists? test-repo-path))
  (is (fs/directory? test-repo-path)))

(deftest list-job-definitions
  (testing "list job definitions head revisions"
    (let [{:keys [status body] :as resp} (client/get "http://localhost:3000/repo/jobdefinitions/heads" transit-request-stub)]
      (is (= status 200))
      (is (map? body))
      (is (contains? body jd-name))
      (is (= (get body jd-name) 2)))))

(deftest get-job-definitions
  (testing "get job definitions head revisions"
    (let [{:keys [status body] :as resp} (client/get "http://localhost:3000/repo/jobdefinitions" transit-request-stub)]
      (is (= status 200))
      (is (map? body))
      (is (contains? body jd-name))
      (is (= (get body jd-name) (assoc job-def :revision 2))))))

(deftest list-job-def-revisions
  (testing "list job definitions revisions"
    (let [{:keys [status body] :as resp} (client/get "http://localhost:3000/repo/jobdefinitions/revisions" transit-request-stub)]
      (is (= status 200))
      (is (map? body))
      (is (contains? body jd-name))
      (testing "revision count"
        (is (= (-> body
                   (get jd-name)
                   count)
               2)))
      (testing "revision note"
        (is (= (-> body
                   (get jd-name)
                   first
                   (get 3))
               revision-note))))))

(deftest get-job-definition
  (testing "get a head revision of a given job def"
    (let [{:keys [status body] :as resp} (client/get (str "http://localhost:3000/repo/jobdefinitions/" jd-name) transit-request-stub)]
      (is (= status 200))
      (is (= body (assoc job-def :revision 2))))))

(deftest get-job-def-revision
  (testing "get specified revision of a given job definition"
    (let [{:keys [status body] :as resp} (client/get (str "http://localhost:3000/repo/jobdefinitions/" jd-name "/" 2) transit-request-stub)]
      (is (= status 200))
      (is (= body (assoc job-def :revision 2))))))

(deftest get-job-definition-err
  (testing "get a head revision of a given job def - testing 404 response if job definition does not exist"
    (let [{:keys [status body] :as resp} (client/get (str "http://localhost:3000/repo/jobdefinitions/" "job definition that does not exist") transit-request-stub)]
      (is (= status 404)))))

(deftest get-job-def-revision-err
  (testing "get specified revision of a given job definition - testing 404 response if revision does not exist"
    (let [{:keys [status body] :as resp} (client/get (str "http://localhost:3000/repo/jobdefinitions/" jd-name "/" 1024) transit-request-stub)]
      (is (= status 404)))))

(deftest system-core-exists-running
  (testing "testing if system :core exists in systems catalogue"
    (let [{:keys [status body] :as resp} (client/get "http://localhost:3000/systems/" transit-request-stub)]
      (is (= status 200))
      (is (contains? body :core))
      (is (= :running (get-in body [:core :state]))))))

(deftest system-core-live-running
  (testing "testing if system :core is running (with 2 workers)"
    (let [{:keys [status body] :as resp} (client/get "http://localhost:3000/systems/live" transit-request-stub)]
      (is (= status 200))
      (is (vector? body))
      (is (contains? (into {} body) :core))
      (is (= :running (get-in (into {} body) [:core :state])))
      (is (= body [[:core {:state :running, :workers [0 1]}]])))))

(deftest system-core-exists-stopped
  (testing "testing if system :core exists in systems catalogue (and is NOT running)"
    (let [{:keys [status body] :as resp} (client/get "http://localhost:3000/systems/" transit-request-stub)]
      (is (= status 200))
      (is (contains? body :core))
      (is (not= :running (get-in body [:core :state]))))))

(deftest system-core-stop
  (testing "stopping system :core"
    (let [{:keys [status body] :as resp} (client/patch "http://localhost:3000/systems/%3Acore" (req {:action :stop}))]
      (is (= status 200))
      (Thread/sleep 50)
      (system-core-exists-stopped))))

(deftest system-core-start
  (testing "starting system :core"
    (let [{:keys [status body] :as resp} (client/patch "http://localhost:3000/systems/%3Acore" (req {:action :start :wcount 2}))]
      (is (= status 200))
      (Thread/sleep 1500)
      (system-core-exists-running)
      (system-core-live-running))))

(deftest create-job
  (testing "starting a new job"
    (let [{:keys [status body] :as resp}  (client/post "http://localhost:3000/systems/%3Acore/jobs" (req {:jobdef-name jd-name}))]
      (is (= status 201))
      (is (instance? java.util.UUID (java.util.UUID/fromString (:jobid body))))
      (swap! jobs-created conj (:jobid body)))))

(deftest create-job-sync
  (testing "starting a new job"
    (let [{:keys [status body] :as resp}  (client/post "http://localhost:3000/systems/%3Acore/jobs" (req {:jobdef-name jd-name
                                                                                                          :sync true}))]
      (is (= status 201))
      (let [{:keys [jobid properties history state]} body]
      (is (instance? java.util.UUID (java.util.UUID/fromString jobid)))
      (is (vector? history))
      (is (map? properties))
      (is (= state :finished))
      (is (= (:message properties) "Hello World!")))
      (swap! jobs-created conj (:jobid body)))))

(defn check-job-result-sync [name expected-message]
  (testing "starting a new job"
    (let [{:keys [status body] :as resp}  (client/post "http://localhost:3000/systems/%3Acore/jobs" (req {:jobdef-name jd-name
                                                                                                          :properties {:name name}
                                                                                                          :sync true}))]
      (is (= status 201))
      (let [{:keys [jobid properties history state]} body]
        (is (instance? java.util.UUID (java.util.UUID/fromString jobid)))
        (is (vector? history))
        (is (map? properties))
        (is (= state :finished))
        (is (= (:message properties) expected-message)))
      (swap! jobs-created conj (:jobid body)))))

(deftest test-job-flow
  (check-job-result-sync "Miro" "Hello Miro!")
  (check-job-result-sync nil "Hello nobody!"))

(deftest get-jobs
  (testing "getting recent jobs"
    (let [{:keys [status body] :as resp}  (client/get "http://localhost:3000/systems/jobs" transit-request-stub)]
      (is (= status 200))
      (is (= @jobs-created
             (-> body
                 :core
                 keys
                 set))))))


(defn test-ns-hook
  "Run tests in a sorted order."
  []
  (remove-repo)
  (repo-not-exists)
  (start-server)

  ;; test job definitions repository API
  (create-job-definition)
  (repo-exists)

  (create-job)

  (update-job-definition)
  (list-job-definitions)
  (get-job-definitions)
  (list-job-def-revisions)
  (get-job-definition)
  (get-job-definition-err)
  (get-job-def-revision)
  (get-job-def-revision-err)

  ;; job creation API
  (create-job)

  ;; systems API
  (system-core-exists-running)
  (system-core-live-running)
  (system-core-stop)
  (system-core-start)

  ;; job creation & retrieval API
  (reset! jobs-created #{})
  (create-job)
  (create-job-sync)
  (test-job-flow)
  (get-jobs)

  ;; shutdown
  (stop-server)
  (remove-repo)
  (repo-not-exists))

;; TODO make also tests for json API: (first will require changes to serialization)
#_(clj-http.client/post "http://localhost:3000/repo/jobdefinitions/test"
                      {:body (clojure.data.json/write-str {:definition {:first-step "step", :name "test", :type nil, :properties {:name "somebody"}, :steps [{:id "step", :type :custom, :supertype :tasklet, :next [[3 "step3"] [2 "step2"]], :workload-fn #titanoboa.exp.Expression{:value " (fn [j] \n   (+ 1 1))", :type nil}, :properties {}} {:id "step2", :type :custom, :supertype :tasklet, :workload-fn #titanoboa.exp.Expression{:value " (fn [j]\n      (+ 2 2))", :type nil}, :next [[4 "step4"] [5 "step5"]], :properties {}} {:id "step3", :type :custom, :supertype :tasklet, :workload-fn #titanoboa.exp.Expression{:value "  (fn [j] \n   \"Hello World!\")", :type nil}, :next [], :properties {}} {:id "step4", :type :custom, :supertype :tasklet, :workload-fn #titanoboa.exp.Expression{:value "( p ) -> {  \n\t\t\tString prop = (String) p.get(\"someProperty\");\n\t\t\tString anotherProp = prop + \" Nice to meet you!\";\n\t\t\tjava.util.HashMap propertiesToReturn = new java.util.HashMap (); \n\t\t\tpropertiesToReturn.put (\"anotherOne\", anotherProp);\n\t\t\treturn clojure.lang.PersistentArrayMap.create(propertiesToReturn);\n\t\t\t}", :type "java"}, :next [], :properties {"someProperty" "Hello World!"}} {:id "step5", :type :custom, :supertype :tasklet, :workload-fn #titanoboa.exp.Expression{:value "  (fn [j] \n   \"Hello World!\")", :type nil}, :next [], :properties {}}]}
                                                           :notes      "loremipsum"})
                       :content-type :json
                       :as :json
                       :transit-opts
                       {:encode {:handlers handler/transit-handlers-encode}
                        :decode {:handlers handler/transit-handlers-decode}}})
