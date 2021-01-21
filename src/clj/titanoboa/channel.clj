; Copyright (c) Miroslav Kubicek. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.channel
  (:require  [com.stuartsierra.component :as component]
             [clojure.core.async :as async :refer [>!! <!!]]
             [clojure.core.async.impl.protocols :refer [ReadPort WritePort]]
             [taoensso.nippy :as nippy]
             [clojure.tools.logging :as log]))

(def ^{:dynamic true} *mq-session* nil)

(defn current-session
  "If used within (with-connection conn ...),
   returns the currently bound connection."
  []
  (when *mq-session*
    *mq-session*))

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
  If the pool is nil or the session has already been bound then does nothing - to enable nested use of this macro without further consumption from the session pool."
  [pool & body]
  `(if (or *mq-session* (nil? ~pool))
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


(defmulti poll!
  "Synchronous poll operation.
  Reads from given channel or immediatelly returns nil if the channel is empty."
  (fn [ch] (class ch)))

(defmethod poll! clojure.core.async.impl.channels.ManyToManyChannel
  [ch]
  (async/poll! ch))

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
  (fn [chan message] (cond
                       (instance? titanoboa.channel.TransChannelProtocol chan) :trans-channel-protocol
                       (instance? clojure.core.async.impl.channels.ManyToManyChannel chan) :core-async
                       :else :default)))

(defmethod nack! :trans-channel-protocol [chan message] (.nack! chan message))

;;nack for core.async is basically just returning the message onto the chan
(defmethod nack! :core-async [chan message] (async/put! chan message))

(defmethod nack! :default [chan message] nil)

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

;;(def subs_ch (async/chan 256))
;;(channel/bind-chan (get-in @system/systems-state [:core :system :jobs-cmd-exchange]) subs_ch "123")
;;(run-job! :core {:jobdef-name "suspend-test" :id "123"} false)
;;(command->job :core "123" "suspend")
;;(get-in @system/systems-state [:core :system :job-state])

(defrecord Exchange [chan exchange routing-fn]
  WritePort
  (put! [this val handler]
    (if handler  (.put! chan val handler) (.put! chan val)))
  ReadPort
  (take! [this handler]
    (.take! chan handler))
  component/Lifecycle
  (start [this]
    (log/info "Creating Pub for internal Exchange channel with routing fn " routing-fn)
    (if exchange
      this
      (assoc this :exchange (async/pub chan routing-fn))))
  (stop [this]
    (async/close! chan)
    (dissoc this :exchange)))

(defmulti bind-chan "Binds (subscribes) the provided channel to the topic exchange using the given routing key. Has to return the bound channel."
          (fn [exchange chan routing-key] (class chan)))

(defmulti unbind-chan "Unbinds (unsubscribes) provided channel from the topic exchange."
          (fn [exchange chan routing-key] (class chan)))

(defmethod bind-chan clojure.core.async.impl.channels.ManyToManyChannel
  [exchange chan routing-key]
  (async/sub (:exchange exchange) routing-key chan)
  chan)

(defmethod unbind-chan clojure.core.async.impl.channels.ManyToManyChannel
  [exchange chan routing-key]
  (async/unsub (:exchange exchange) routing-key chan))

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

(defrecord SerializedVar [symbol]) ;; TODO add node-id?


(nippy/extend-freeze clojure.lang.Var :clojure.core/var
                     [x data-output]
                     (.writeUTF data-output (str x)))

(nippy/extend-thaw :clojure.core/var
                   [data-input]
                   (->SerializedVar (.readUTF data-input)))

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