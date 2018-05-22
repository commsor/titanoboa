;; Anything you type in here will be executed
;; immediately with the results shown on the
;; right.
(use 'titanoboa.processor)
(require 'langohr.basic)
(require 'langohr.core)
(require 'langohr.channel)
(require 'taoensso.nippy)
(require '[clojure.core.async.impl.protocols :refer [ReadPort WritePort take!]])

#_(preprocess nil)


@job-defs




#_(load-job-def "c:/workspace-sts-3.5.1.RELEASE/titanoboa/resources/test/simple-job.edn")

#_(start-action-processor 1)

#_(start-job-precessor 1)

#_(clojure.core.async/>!! action-requests-ch action-request)



;;@job-defs



(def con (langohr.core/connect))

(def ch (langohr.channel/open con))

(langohr.channel/close ch)

(langohr.core/close con)

#_(langohr.basic/publish ch "" "titanoboa-bus-queue-1" "test message"
                       {:content-type "text/plain" :type "test"})

#_(defn rabbit-chan
  [ch queue]
  (reify
    WritePort
    (put! [_ val _]
      (atom (langohr.basic/publish ch "" queue (taoensso.nippy/freeze val) {:content-type "application/octet-stream" :type "titanoboa"})))))

(require '[clojure.core.async :refer [>!!]])

(def rch (rabbit-chan ch "titanoboa-bus-queue-1"))

(def cch (rabbit-chan ch "titanoboa-node1-worker1"))

(>!! rch ["tralalaX"])

(time (>!! cch [":D"]))

(String. (get (langohr.basic/get ch "titanoboa-bus-queue-1") 1))

(if (langohr.basic/get ch "titanoboa-bus-queue-1") "haha" "hoho")

(time (langohr.basic/get ch "titanoboa-bus-queue-1"))

(loop []
  (let [m (langohr.basic/get ch "titanoboa-bus-queue-1")]
    (if m
      m
      (do
        (Thread/sleep 100)
        (recur)))))


#_(def message (langohr.basic/get ch "titanoboa-bus-queue-1"))

(String. (get message 1))

#_(taoensso.nippy/thaw
 (taoensso.nippy/freeze @job-defs))

(.getTime (java.util.Date.))

(let [cal (java.util.Calendar/getInstance)] (.add cal java.util.Calendar/MONTH 1) cal)

@running-jobs

(get-in  (vec (vals @running-jobs)) [0 :properties])

(count @finished-jobs)

(get-in  (vec (vals @finished-jobs)) [1 :properties])


;;------------------------------------------

(load-job-def "c:/workspace-sts-3.5.1.RELEASE/titanoboa/resources/test/simple-job.edn")

@job-defs

(vals @job-defs)

(deref job-defs)

(def action-requests-ch (chan (dropping-buffer 32)))
(def response-ch (chan 8))
(def finished-ch (chan (dropping-buffer 64)))

(start-action-processor 1 action-requests-ch)


(def con (langohr.core/connect))

(def ch (langohr.channel/open con))

(def jobs-rmqch (channel/rabbit-chan ch "titanoboa-bus-queue-1"))

(def new-jobs-rmqch (channel/rabbit-chan ch "titanoboa-bus-queue-0"))

(def action-request {:action-fn instantiate-job
                  :data ["simple job" nil nil new-jobs-rmqch running-jobs job-folder]
                  :response-ch response-ch})

(instantiate-job "simple job" nil nil new-jobs-rmqch running-jobs job-folder)

(>!! action-requests-ch action-request)

(process-from-ch! jobs-rmqch new-jobs-rmqch jobs-rmqch finished-ch running-jobs)


(def stop-ch (chan 8))

(start-processor! stop-ch jobs-rmqch new-jobs-rmqch jobs-rmqch finished-ch running-jobs)

(>!! stop-ch :stop)

(<!!  response-ch)
(<!! finished-ch)

(require 'langohr.queue)

(time (langohr.queue/declare ch "test-queue" {:exclusive false :auto-delete true}))


;;*************************************

(def core-system (local-core-system {}))

(alter-var-root #'core-system component/start)
(alter-var-root #'core-system component/stop)

(def worker-system-1 (local-worker-system core-system))

(alter-var-root #'worker-system-1 component/start)
(alter-var-root #'worker-system-1 component/stop)

(processor/load-job-def! "c:/workspace-sts-3.5.1.RELEASE/titanoboa/resources/test/simple-job.edn" (:job-defs core-system))

(dotimes [n 5]
(def response-ch (chan 1))
(def action-request {:action-fn processor/run-sync-job!
                  :data [{:jobdef-name "simple job"
                          :new-jobs-ch (:new-jobs-chan core-system)
                          :state-agent (:job-state core-system)
                          :job-folder "c:/workspace-sts-3.5.1.RELEASE/titanoboa/resources/test/"
                          :defs-atom (:job-defs core-system)
                          :mq-pool (get-in core-system [:mq-session-pool :pool])}]
                  :response-ch response-ch})

(time
  (do (>!! (:action-chan core-system) action-request)
  (<!! response-ch))))

(:history ((deref (:job-state core-system))))

#_(processor/instantiate-job! {:jobdef-name "simple job"
                          :new-jobs-ch new-jobs-rmqch
                          :state-agent (:job-state core-system)
                          :job-folder "c:/workspace-sts-3.5.1.RELEASE/titanoboa/resources/test/"
                          :defs-atom (:job-defs core-system)})

(channel/with-mq-session (get-in worker-system-1 [:mq-session :session])
                         (>!! new-jobs-rmqch {:test :tralala}))

(channel/with-mq-session (get-in worker-system-1 [:mq-session :session])
                         (<!! new-jobs-rmqch))

(<!! new-jobs-rmqch)

(def rmq-pool (channel/create-rmq-pool (langohr.core/connect) 4))

rmq-pool

(channel/with-mq-sessionpool rmq-pool
                         (>!! new-jobs-rmqch {:test :tralala}))

(channel/with-mq-sessionpool rmq-pool
                         (println (<!! new-jobs-rmqch)))

;;testing serialization of rmq channel:

(def con (langohr.core/connect))

(def ch (langohr.channel/open con))

(<!! new-jobs-rmqch)

(channel/with-mq-session ch
                         (>!! new-jobs-rmqch {:test :tralala}))

(channel/with-mq-session ch
                         (println (<!! (<!! new-jobs-rmqch))))

(channel/with-mq-session ch
                         (>!! new-jobs-rmqch new-jobs-rmqch))

(>!! response-ch nil)

;;--------------

(def core-system (local-core-system {}))

(alter-var-root #'core-system component/start)
(alter-var-root #'core-system component/stop)

(def worker-system-1 (local-worker-system core-system))

(alter-var-root #'worker-system-1 component/start)
(alter-var-root #'worker-system-1 component/stop)

(processor/load-job-def! "c:/workspace-sts-3.5.1.RELEASE/titanoboa/resources/test/simple-job.edn" (:job-defs core-system))
(processor/load-job-def! "c:/workspace-sts-3.5.1.RELEASE/titanoboa/resources/test/simple-job-copy.edn" (:job-defs core-system))

(def response-ch (chan 1))
(def action-request {:action-fn processor/run-sync-job!
                  :data [{:jobdef-name "vivanet job"
                          :new-jobs-ch (:new-jobs-chan core-system)
                          :state-agent (:job-state core-system)
                          :job-folder "c:/workspace-sts-3.5.1.RELEASE/titanoboa/resources/test/"
                          :defs-atom (:job-defs core-system)
                          :mq-pool (get-in core-system [:mq-session-pool :pool])}]
                  :response-ch response-ch})

(time
  (do (>!! (:action-chan core-system) action-request)
  (<!! response-ch)))

(def response-ch (chan 1))
(def action-request {:action-fn processor/instantiate-job!
                  :data [{:jobdef-name "vivanet job"
                          :new-jobs-ch (:new-jobs-chan core-system)
                          :state-agent (:job-state core-system)
                          :job-folder "c:/workspace-sts-3.5.1.RELEASE/titanoboa/resources/test/"
                          :defs-atom (:job-defs core-system)
                          :mq-pool (get-in core-system [:mq-session-pool :pool])}]
                  :response-ch response-ch})

(>!! (:action-chan core-system) action-request)

;;+++++++++++++++++++++++++++++++++++++++

(def core-system (local-core-system {}))

(alter-var-root #'core-system component/start)
(alter-var-root #'core-system component/stop)

(def worker-system-1 (local-worker-system core-system))

(alter-var-root #'worker-system-1 component/start)
(alter-var-root #'worker-system-1 component/stop)

(def worker-system-2 (local-worker-system core-system))

(alter-var-root #'worker-system-2 component/start)
(alter-var-root #'worker-system-2 component/stop)

(def worker-system-3 (local-worker-system core-system))

(alter-var-root #'worker-system-3 component/start)
(alter-var-root #'worker-system-3 component/stop)

(def worker-system-4 (local-worker-system core-system))

(alter-var-root #'worker-system-4 component/start)
(alter-var-root #'worker-system-4 component/stop)

(def worker-system-5 (local-worker-system core-system))

(alter-var-root #'worker-system-5 component/start)
(alter-var-root #'worker-system-5 component/stop)

(def worker-system-6 (local-worker-system core-system))

(alter-var-root #'worker-system-6 component/start)
(alter-var-root #'worker-system-6 component/stop)

(def worker-system-7 (local-worker-system core-system))

(alter-var-root #'worker-system-7 component/start)
(alter-var-root #'worker-system-7 component/stop)

(def worker-system-8 (local-worker-system core-system))

(alter-var-root #'worker-system-8 component/start)
(alter-var-root #'worker-system-8 component/stop)

(processor/load-job-def! "c:/workspace-sts-3.5.1.RELEASE/titanoboa/resources/test/simple-job-copy.edn" (:job-defs core-system))


(time
(do
(dotimes [n 100]
(def response-ch (chan 1))
(def action-request {:action-fn processor/run-sync-job!
                  :data [{:jobdef-name "vivanet job"
                          :new-jobs-ch (:new-jobs-chan core-system)
                          :state-agent (:job-state core-system)
                          :job-folder "c:/workspace-sts-3.5.1.RELEASE/titanoboa/resources/test/"
                          :defs-atom (:job-defs core-system)
                          :mq-pool (get-in core-system [:mq-session-pool :pool])}]
                  :response-ch response-ch})
  (>!! (:action-chan core-system) action-request))
(<!! response-ch)))

(def action-request {:action-fn titanoboa.processor/run-sync-job!
                  :data [{:jobdef-name "simple-ondemand"
                          :new-jobs-ch (:new-jobs-chan core-system)
                          :state-agent (:job-state core-system)
                          :job-folder "c:/Users/mkubicek/Dropbox/titanoboa/resources/test/"
                          :defs-atom (:job-defs core-system)
                          :mq-pool (get-in core-system [:mq-session-pool :pool])}]
                  :response-ch response-ch})
