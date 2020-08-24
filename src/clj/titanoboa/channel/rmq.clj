;   Copyright (c) Miroslav Kubicek. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
;   which can be found in the LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns titanoboa.channel.rmq
  (:require  [com.stuartsierra.component :as component]
             [clojure.core.async :as async :refer [>!! <!!]]
             [clojure.core.async.impl.protocols :refer [ReadPort WritePort]]
             [titanoboa.channel :as ch :refer [with-mq-session with-mq-sessionpool current-session TransChannelProtocol Cleanable]]
             [langohr.basic]
             [langohr.core :as langohr]
             [langohr.channel]
             [langohr.queue]
             [langohr.consumers]
             [langohr.exchange]
             [langohr.http]
             [taoensso.nippy :as nippy]
             [clojure.tools.logging :as log]))


(nippy/extend-freeze clojure.lang.Var :clojure.core/var
                     [x data-output]
                     (.writeUTF data-output (str x)))

(nippy/extend-thaw :clojure.core/var
                   [data-input]
                   (ch/->SerializedVar (.readUTF data-input)))

(nippy/extend-freeze clojure.core.async.impl.channels.ManyToManyChannel :core-async-chan
                     [x data-output]
                     nil)

(nippy/extend-thaw :core-async-chan
                   [data-input]
                   :core-async-chan)
;;FIXME cast this into normal Exception upstream
(nippy/extend-freeze pl.joegreen.lambdaFromString.LambdaCreationException :lambda-creation-exception
                     [x data-output]
                     (.writeUTF data-output (.getMessage x)))

(nippy/extend-thaw :lambda-creation-exception
                   [data-input]
                   (java.lang.Exception. (.readUTF data-input)))


(nippy/set-freeze-fallback!
  (fn [data-output x]
    (let [s (str x)
          ba (.getBytes s "UTF-8")
          len (alength ba)]
      (.writeByte data-output (byte 13))
      (.writeInt data-output (int len))
      (.write data-output ba 0 len))))

;;interesting bug:
;;throw+  applied to returned clj-http.headers/->HeaderMap cannot be serialized
#_(-> (try (slingshot.slingshot/throw+ (get resp :headers) "status %s" (:status %))
           (catch Exception e
             e))
      nippy/freeze)

#_(def wfn (.createLambda (LambdaFactory/get)
                          "p -> {Integer b = (Integer) p.valAt(\"a\");
                          b++;
                          return p.assoc(\"b\", b);}"
                          "Function< clojure.lang.PersistentArrayMap,  clojure.lang.IPersistentMap>"))
;;=> #'titanoboa.server/wfn
;;(.apply wfn {"a" (int 0)})
;;=> {"a" 0, "b" 1}

(defn create-rmq-pool [connection n]
  "Instantiates a RabbitMQ session pool of size n.
  Returns core.async channel of size n that contains n maps in a format of {:connection RMQ-Connection-object :session RMQ-Session-object}.
  If a connection is provided, all these session are using this connection.
  TODO: If URI is provided instead of a connection, each session will has its unique connection."
  (let [pool (async/chan n)]
    (doseq [_ (range n)]
      (async/>!! pool {:connection connection :session (langohr.channel/open connection)}))
    pool))

(defn close-mq-pool! [pool]
  "Closes all (unused!) sessions from the provided pool. Will not close the connection.
  If any session from the given pool is currently being used,
  it is kept opened but will never be returned to this pool as the pool will be closed."
  (loop []
    (let [c-map (async/poll! pool)]
      (if c-map
        (do (langohr.core/close (:session c-map))
            (recur))
        (async/close! pool)))))

(defn rmq-poll!
  "Returns a message from specified RabbitMQ queue. If the queue is empty then returns nil.
  Attaches header map as messages metadata.
  Non-clojure objects (like e.g. java.util.String) are therefore not supported and can't be used as messages!"
  ([session queue auto-ack]
   (let [m (langohr.basic/get session queue auto-ack)]
     (if m
       (with-meta (nippy/thaw (peek m)) (get m 0))
       nil)))
  ([{:keys [queue auto-ack]}]
   (rmq-poll! (current-session) queue auto-ack)))


(defn rmq-blocking-poll! [{:keys [queue auto-ack poll-interval] :as rmq-opts} & [stop-chan]]
  "Returns a message from specified RabbitMQ queue.
  If the queue is empty then blocks until a new message is available; the thread will sleep for poll-interval and then will poll agian.
  If the core.async stop-chan is provided it can be used to interrupt the polling loop.
  Any value (incl. nil) retrieved from the stop-chan will stop the loop and this method will return nil."
  (loop []
    (if (and stop-chan (not= (async/alts!! [stop-chan] :default nil) [nil :default]))
      nil
      (let [m (rmq-poll! rmq-opts)]
        (if m
          m
          (do
            (Thread/sleep poll-interval)
            (recur)))))))

;;potentially we can add an attribute 'session' into this record which would be used instead of the *session* binding - in case there is no binding in place etc.
(defrecord RMQPollingChannel [queue auto-ack poll-interval exchange]
  WritePort
  (put! [_ val _]
    (atom (langohr.basic/publish (current-session)  (or exchange "") queue (nippy/freeze val) {:content-type "application/octet-stream" :type "titanoboa"})))
  ReadPort
  (take! [this handler]
    (atom (rmq-blocking-poll! (assoc this :session (current-session)))))
  TransChannelProtocol
  (ack! [_ message]
    (if auto-ack
      nil
      (langohr.basic/ack (current-session) (:delivery-tag (meta message)) false)))
  (nack! [_ message]
    (if auto-ack
      nil
      (langohr.basic/nack (current-session) (:delivery-tag (meta message)) false true))))

(defn rmq-chan
  "Creates an experimental polling async chanel from a RabbitMQ queue.
  If queue name is not provided, attempts to create a new queue with an auto-generated name - note that this operation may require an open session to succeed.
  Currently only >!! and <!! operations are supported and the channel cannot be used in go block or in alts!! command."
  ([queue auto-ack poll-interval & [exchange]]
   (let [queue (or queue (:queue (langohr.queue/declare (current-session) "" {:exclusive false :auto-delete false})))]
     (->RMQPollingChannel queue auto-ack poll-interval exchange)))
  ([queue auto-ack] (rmq-chan queue auto-ack 100))
  ([queue] (rmq-chan queue true 100))
  ([] (rmq-chan nil true 100)))

(defmethod ch/mq-chan com.rabbitmq.client.impl.recovery.AutorecoveringChannel
  [queue auto-ack]
  (rmq-chan queue auto-ack))

(defmethod ch/delete-mq-chan RMQPollingChannel
  [chan]
  (langohr.queue/delete (current-session) (:queue chan)))

(defrecord RMQExchangeBroadcast [exchange expiration headers]
  WritePort
  (put! [_ val _]
    (atom (langohr.basic/publish (current-session)  exchange "" (nippy/freeze val) {:content-type "application/octet-stream" :type "titanoboa" :expiration expiration :headers headers})))
  ReadPort
  (take! [this handler]
    (throw (UnsupportedOperationException. "Cannot consume messages directly from RMQ Fan Exchange! Use RMQExchangeSubscriber instead."))))

(defmethod ch/->distributed-ch com.rabbitmq.client.impl.recovery.AutorecoveringChannel
  [chan]
  "Enables distributed callback accross a different address space / servers.
  Creates (a serializable) channel (RMQPollingChannel) for a callback that will be bound to the provided (local) core.async channel.
  This rabbitmq channel can be sent to different address spaces / servers. Once a value is sent to this channel,
  it will be passed onto the provided core.async channel.
  Only one message will be consumed.
  After that the underlying temporary queue and consumer will be cancelled and the provided core.async chan will be closed."
  (let [{:keys [queue]} (langohr.queue/declare (current-session) "" {:exclusive true :auto-delete true})]
    (langohr.consumers/subscribe (current-session) queue
                                 (fn [ch header payload]
                                   (async/>!! chan (with-meta (nippy/thaw payload) header))
                                   (langohr.basic/cancel ch queue)
                                   (async/close! chan))
                                 {:auto-ack true :consumer-tag queue})
    (rmq-chan queue)))


#_(defn distributed? [channel]
  "Returns true if provided channel is serializable and can be distributed to different address spaces.
  Returns false if it is core.assync channel which cannot be serialized.
  Throws IllegalArgumentException if provided parameter is not known channel."
  (condp instance? channel
    clojure.core.async.impl.channels.ManyToManyChannel false
    titanoboa.channel.rmq.RMQPollingChannel true
    (throw (IllegalArgumentException. "Provided channel is not a supported type of channel."))))

(defmethod ch/distributed? titanoboa.channel.rmq.RMQPollingChannel
  [ch]
  true)

(defmethod ch/poll! titanoboa.channel.rmq.RMQPollingChannel
  [ch]
  (rmq-poll! ch))

#_(defmethod ch/poll! clojure.lang.PersistentArrayMap ;;FIXME this is NOT cool! remove this!
  [ch]
  (rmq-poll! ch))

(defn sync-alts!! [rmq-chans & {:keys [priority] :as opts}]
  "Synchronous alts!! for Rabbit MQ and/or for async.channels. Since it is synchronous the :default options is not supported."
  (loop [rmq-chans (if-not priority (shuffle rmq-chans) rmq-chans)]
    (if-let [m (ch/poll! (first rmq-chans))]
      [m (first rmq-chans)]
      (if-not (empty? (rest rmq-chans))
        (recur (rest rmq-chans))
        nil))))

(defn rmq-blocking-alts!! [rmq-chans
                           & {:keys [stop-chan poll-interval default] :or {poll-interval 100} :as opts}]
  "Blocking alts!! for RabbitMQ queues. Also works with async.channels.
  The :default options is not supported - use sync-alts!! instead."
  (let [arg-list (conj (mapcat identity opts) rmq-chans)]
    (loop []
      (if (and stop-chan (not= (async/alts!! [stop-chan] :default nil) [nil :default]))
        nil
        (let [m (apply sync-alts!! arg-list)] ;;(rmq-alt rmq-chans opts)
          (if m
            m
            (do
              (Thread/sleep poll-interval)
              (recur))))))))

(defmethod ch/alts!! titanoboa.channel.rmq.RMQPollingChannel [ & args]
  (apply rmq-blocking-alts!! args))

;;multithreaded blocking/polling alt could also be handled like this:
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


(defrecord RMQConnectionComponent [params connection]
  component/Lifecycle
  (start [this]
    (log/info "Instantiating RabbitMQ Connection...")
    (if connection
      this
      (assoc this
        :connection (langohr/connect params))))
  (stop [this]
    (log/info "Closing RabbitMQ Connection...")
    (try (langohr.core/close connection)
         (catch com.rabbitmq.client.AlreadyClosedException e
           (log/info "RabbitMQ Session was already closed.")))
    (assoc this
      :connection nil)))

(defrecord RMQSessionComponent [connection-comp session]
  component/Lifecycle
  (start [this]
    (log/info "Instantiating RabbitMQ Session...")
    (if session
      this
      (assoc this
        :session (langohr.channel/open (:connection connection-comp)))))
  (stop [this]
    (log/info "Closing RabbitMQ Session...")
    (try (langohr.core/close session)
         (catch com.rabbitmq.client.AlreadyClosedException e
           (log/info "RabbitMQ Session was already closed.")))
    (assoc this
      :session nil)))

(defrecord RMQSessionPoolComponent [connection-comp pool-size pool]
  component/Lifecycle
  (start [this]
    (log/info "Instantiating RabbitMQ Session Pool...")
    (if pool
      this
      (assoc this
        :pool (create-rmq-pool (:connection connection-comp) pool-size))))
  (stop [this]
    (log/info "Closing RabbitMQ Session Pool...")
    (close-mq-pool! pool)
    (assoc this
      :pool nil)))

(defrecord QueueComponent [session-comp sessionpool-comp queue-name]
  component/Lifecycle
  (start [this]
    (log/info "Creating queue [" queue-name "]...")
    (if session-comp
      (with-mq-session (:session session-comp)
                       (langohr.queue/declare (current-session) queue-name {:exclusive false :auto-delete false :durable true}))
      (when sessionpool-comp
        (with-mq-sessionpool (:pool sessionpool-comp)
                             (langohr.queue/declare (current-session) queue-name {:exclusive false :auto-delete false :durable true}))))
    this)
  (stop [this]
    this)
  Cleanable
  (cleanup! [this]
    (langohr.queue/delete (current-session) queue-name)))
