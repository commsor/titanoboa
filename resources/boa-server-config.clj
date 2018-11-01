(in-ns 'titanoboa.server)
(log/info "Hello, I am core.async server-config and I am being loaded...")
(defonce archival-queue-local (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024)))
(alter-var-root #'server-config
                (constantly {:systems-catalogue
                                              {:core  {:system-def #'titanoboa.system.local/local-core-system
                                                       :worker-def #'titanoboa.system.local/local-worker-system
                                                       :autostart  true}}
                             :job-folder-path "job-folders/"
                             :enable-cluster false
                             :jetty {:join? false
                                     :port 3000}
                             :auth? false
                             :systems-config {:core
                                              {:new-jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024))
                                               :jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024))
                                               :finished-jobs-chan archival-queue-local
                                               :eviction-interval (* 1000 60 5)
                                               :eviction-age (* 1000 60 60)}}}))