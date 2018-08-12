(in-ns 'titanoboa.server)
(log/info "Hello, I am core.async server-config and I am being loaded...")
(defonce archival-queue-local (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024)))
(alter-var-root #'server-config
                (constantly {:systems-catalogue
                                               {:core  {:system-def #'titanoboa.system.local/local-core-system
                                                        :worker-def #'titanoboa.system.local/local-worker-system
                                                        :worker-count 2
                                                        :autostart  true}
                                                :archival-system {:system-def #'titanoboa.system.local/archival-system
                                                                  :autostart  true}}
                             :jobs-repo-path "dev-resources/repo/"
                             :steps-repo-path "dev-resources/step-repo/"
                             :job-folder-path "test/"
                             :enable-cluster false
                             :jetty {:join? false
                                     :port 3000}
                             :archive-ds-ks [:archival-system :system :db-pool]
                             :auth? false
                             :systems-config {:core
                                              {:new-jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024))
                                               :jobs-chan (clojure.core.async/chan (clojure.core.async/dropping-buffer 1024))
                                               :finished-jobs-chan archival-queue-local}
                                              :archival-system
                                              {:jdbc-url "jdbc:postgresql://localhost:5432/mydb?currentSchema=titanoboa"
                                               :finished-jobs-chan archival-queue-local}}}))

