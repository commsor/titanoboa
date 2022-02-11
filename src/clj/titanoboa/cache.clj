; Copyright (c) Commsor Inc. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.cache
  (:require [com.stuartsierra.component :as component]
  [clojure.tools.logging :as log]))

;;TODO dont use separate eviction-agent - just use metadata to mark jobs for eviction in the job-cache-agent -> this will eliminate the race condition or need for STM
(defrecord CacheEvictionComponent [thread-handle eviction-interval eviction-age eviction-agent job-cache-agent]
  component/Lifecycle
  (start [this]
    (log/info "Starting CacheEvictionComponent...")
    (if thread-handle
      this
      (let [th (Thread. (fn[]
                          (log/info "Starting CacheEvictionComponent thread [" (.getName (Thread/currentThread)) "].")
                          (loop [t (.getTime (java.util.Date.))]
                            (let [keys-to-evict (->> @eviction-agent
                                                     vec
                                                     (filter (fn [[k v]]
                                                               (>= (- t (.getTime v)) eviction-age)))
                                                     (mapv first))]
                              (when (and keys-to-evict (not-empty keys-to-evict))
                                (log/info "Evicting jobs from cache: [" keys-to-evict "].")
                                ;;NOTE: since STM is not used there will be race conditions, but since the cache is used only for GUI some inconsistencies are considered acceptable:
                                (send job-cache-agent #(apply dissoc % keys-to-evict))
                                (send eviction-agent #(apply dissoc % keys-to-evict))))
                            (Thread/sleep eviction-interval)
                            (recur (.getTime (java.util.Date.)))))
                        (str "CacheEvictionComponent thread " (rand-int 9)))]
        (.start th)
        (assoc this
          :thread-handle th))))
  (stop [this]
    (log/info "Stopping CacheEvictionComponent thread [" (.getName thread-handle) "]...")
    (if thread-handle (.interrupt thread-handle))
    (assoc this
      :thread-handle nil)))