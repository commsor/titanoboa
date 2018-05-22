(ns titanoboa.channel
  (:require  [com.stuartsierra.component :as component]
             [clojure.core.async :as async :refer [>!! <!!]]
             [clojure.core.async.impl.protocols :refer [ReadPort WritePort]]
             [taoensso.nippy :as nippy]
             [clojure.tools.logging :as log]))

;;TODO create a protocol for channel (if doesnt already exist), identify which functions are generic and and move the rest as a concrete RMQ implementation under titanoboa.system.rmq.channel
;;TODO then implement support for ActiveMQ / HornetMQ / WebSphereMQ
(def ^{:dynamic true} *mq-session* nil)

(defn current-session
  "If used within (with-connection conn ...),
   returns the currently bound connection."
  []
  (if *mq-session*
    *mq-session*
    nil
    #_(throw (IllegalStateException.
      "No current session. Use (with-mq-session conn ...) to bind a session."))))

(defmacro with-mq-session
  "Binds session to a value you can retrieve
   with (current-session) within body."
  [conn & body]
  `(binding [*mq-session* ~conn]
     ~@body))

(defmacro with-mq-sessionpool
  "Retrieves a session from provided session pool (which is expected to be a simple core.async chan
  that provides maps in a format of {:connection RMQ-Connection-object :session RMQ-Session-object}).
  Binds session to a value you can retrieve
  with (current-session) within body.
  If the session has already been bound then does nothing - to enable nested use of this macro without furhter consumption from the session pool."
  [pool & body]
  `(if *mq-session*
     ~@body
     (let [c# (async/<!! ~pool)]
       (binding [*mq-session* (:session c#)]
         ~@body
         (async/>!! ~pool c#)))))


(defprotocol Cleanable
  (cleanup! [this]))

(defprotocol TransChannelProtocol
  "generic interface for channel implementaiton - from core.async through JMS to AMQP"
  (ack! [this message] "Acknowledges given message's delivery.")
  (nack! [this message] "Negative acknowledgement aka rollback."))



;;TODO - if extensibility is required then use records instead of multimethods
;;and specify sync<!! and async<!! atc. methods as part of these records
;;instead of core.async/<!! we would then call my async<!! on such record (it would be neede to implement such protocol even for core.async)

(defmulti poll!
  "Synchronous poll operation.
  Reads from given channel or immediatelly returns nil if the channel is empty."
  (fn [ch] (class ch)))

(defmethod poll! clojure.core.async.impl.channels.ManyToManyChannel
  [ch]
  (async/poll! ch))



;;multithreaded bocking/polling alt could also be handled like this:
#_(let [a (atom false)
      lock (Object.)
      abort-chan (async/chan)]
  (async/thread (do (rmq-blocking-get rch abort-chan)
                  (locking lock
                    (if-not @a
                      (do
                        (swap! a not)
                        (println "thread 1 - Finished 1st")
                        (async/close! abort-chan))
                      (println "thread 1 - Finished 2nd")))))
  (async/thread (do (rmq-blocking-get cch abort-chan)
                  (locking lock
                    (if-not @a
                      (do
                        (println "thread 2 - Finished 1st")
                        (swap! a not)
                        (async/close! abort-chan))
                      (println "thread 2 - Finished 2nd"))))))

;;TODO implement non polling JMS alt (i.e. multiple non-polling threads in wait)
;;TODO allow for different providers (JMS, SQS, AMPQ) to be used in single alt
(defmulti alts!!
  "Enables calling alts!! on a mix of core.async and RabbitMQ channels.
  If only core.async channels are provided it calls the native core.async alts!! function on them.
  Otherwise calls rmq-blocking-alts!! which will block and periodically poll all given channels."
  (fn [ports & {:as opts}] (if
                             (every? #(instance? clojure.core.async.impl.channels.ManyToManyChannel %) ports)
                             clojure.core.async.impl.channels.ManyToManyChannel
                             (-> (filter #(not (instance? clojure.core.async.impl.channels.ManyToManyChannel %)) ports)
                                 first
                                 class))))

(defmethod alts!! clojure.core.async.impl.channels.ManyToManyChannel [ & args]
  (apply async/alts!! args))

(defmacro alt!!
  [& clauses]
  (async/do-alt `alts!! clauses))

(defmulti ack!
  (fn [chan message] (instance? titanoboa.channel.TransChannelProtocol chan)))

(defmethod ack! true [chan message] (.ack! chan message))

(defmethod ack! false [chan message] nil)

(defmulti nack!
  (fn [chan message] (instance? titanoboa.channel.TransChannelProtocol chan)))

(defmethod nack! true [chan message] (.nack! chan message))

(defmethod nack! false [chan message] nil)

(defmulti distributed?
          (fn [ch] (class ch)))

(defmethod distributed? clojure.core.async.impl.channels.ManyToManyChannel
  [ch]
  false)

(defmulti mq-chan
          (fn [queue auto-ack] (class (current-session))))

(defmethod mq-chan nil
  [_ _]
  (async/chan (async/sliding-buffer 256)))

(defmulti delete-mq-chan
          (fn [chan] (class chan)))

(defmethod delete-mq-chan clojure.core.async.impl.channels.ManyToManyChannel
  [chan]
  (async/close! chan))

(defmulti ->distributed-ch
          (fn [ch] (class (current-session))))

(defmethod ->distributed-ch nil
  [ch]
  ch)

(defrecord ExchangeSubsProcessor [processing-fn subs-ch stop-chan]
  component/Lifecycle
  (start [this]
    (log/info "Starting ExchangeSubsProcessor...")
    (if-not stop-chan
      (let [stop-chan (async/chan (async/dropping-buffer 1))
            this (assoc this :stop-chan stop-chan)]
        ;;TODO start subs to the queue here or in separate component?
        (async/thread
          (loop []
            (async/alt!!
              stop-chan :stopped
              subs-ch ([m]
                        (try
                          (processing-fn m)
                          (catch Exception e
                            (log/error e "Error during processing of channel subscription.")))
                        (recur))
              :priority true)))
        this)))
  (stop [this]
    (log/info "Stopping SysStateSubsProcessor... ")
    (>!! stop-chan :stop)
    (async/close! stop-chan)
    (assoc this :stop-chan nil)))

#_(defrecord AsyncChanComponent [size channel]
  component/Lifecycle
 (start [this]
   (assoc this :channel (async/chan size)))
 (stop [this]
   (async/close! channel)))


;;(langohr.exchange/declare (current-session) name "fanout" {:durable true})
#_(nippy/thaw (nippy/freeze
              (merge-with merge
                          (into {} (titanoboa.system/live-systems)) titanoboa.handler/systems-catalogue)))

#_(nippy/set-freeze-fallback!
  (fn [data-output x]
    (let [s (str x)
          ba (.getBytes s "UTF-8")
          len (alength ba)]
      (.writeByte data-output (byte 13))
      (.writeInt data-output (int len))
      (.write data-output ba 0 len))))
