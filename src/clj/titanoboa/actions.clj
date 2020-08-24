; Copyright (c) Miroslav Kubicek. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.actions
  (:require [clojure.core.async :as async :refer [go-loop go >! <! >!! <!! dropping-buffer]]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]))


#_(defn start-action-processor [threadpool-size action-requests-ch]
  (dotimes [n threadpool-size]
    (go-loop []
             (let [{:keys [action-fn data response-ch] :as action-request} (<! action-requests-ch)]
               (>! response-ch (apply action-fn data))
               (recur)))))

;;TODO adjust action processing for distributed environments? (i.e. sending chan over a queue won't work!)
;;-might not be necessary at the moment - each server will process its own actions locally
(defn start-action-processor [threadpool-size action-requests-ch]
  "Starts pool of action processors processing actions from action-requests-ch. returns async channel for stopping the processors.
  Action request is a map with fn to be invoked, its parameters and response channel. Sample action may look as follows:
  {:action-fn instantiate-job :data ['simple job' nil nil new-jobs-rmqch running-jobs job-folder] :response-ch response-ch}.
  Parameter response-ch in the map should contain core.async channel that will be used for response broadcast
  and should be used only once to avoid race conditions. The action-fn itself should never return nil."
  (let [stop-ch (async/chan (dropping-buffer threadpool-size))]
    (dotimes [n threadpool-size]
      (go-loop []
               (async/alt! action-requests-ch
                            ([{:keys [action-fn data response-ch keep-open] :as action-request}]
                             (log/debug (str "Retrieved new action request: " action-request))
                             (>! response-ch (<! (async/thread (apply action-fn data)))) ;;FIXME add error handling
                             (when-not keep-open (async/close! response-ch))
                             (recur))
                            stop-ch
                            ([_] :no-op))))
    stop-ch))

(defrecord ActionProcessorComponent [threadpool-size action-requests-ch stop-ch]
  component/Lifecycle
 (start [this]
   (log/info "Starting action processor pool...")
   (assoc this :stop-ch (start-action-processor threadpool-size action-requests-ch )))
 (stop [this]
   (log/info "Stopping action processor pool...")
   (dotimes [n threadpool-size]
       (>!! stop-ch :stop))
    (async/close! stop-ch)))

