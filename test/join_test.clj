 (ns join-test
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


(deftest start-system!
  (titanoboa.system/start-system! :core-local
                                  {:core-local {:system-def   #'titanoboa.system.local/local-core-system
                                                :worker-def   #'titanoboa.system.local/local-worker-system
                                                :autostart    true
                                                :worker-count 6}}
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
                                                 :worker-count 6}}))
(deftest run-job-test-joins
  (let [{:keys [properties state]}(titanoboa.processor/run-job! :core-local
                                                                {:jobdef     job-def-flow
                                                                 :properties {}}
                                                                true)]
    (is (= state :finished))
    (is (= (:a properties) 5))))

(deftest stop-system!
  (titanoboa.system/stop-all-systems!))

(defn test-ns-hook
  "Run tests in a sorted order."
  []
  (start-system!)
  (start-workers!)
  (run-job-test-joins)
  (stop-system!))

