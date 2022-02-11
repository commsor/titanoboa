; Copyright (c) Commsor Inc. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.api
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [>!! <!! chan]]
            [titanoboa.system :as system]
            [titanoboa.util :as util]
            [titanoboa.processor]))

(defn get-jobs-states []
  "Retrievs job state snapshot from all running core systems.
  Since core systems are not flagged and any system can be a 'core' system,
  the functions simply takes all systems that are running and have referencable :job-state attribute.
  Returns map of job state maps where keys are names of the systems."
  (->> @system/systems-state
       (filter (fn [[k v]] (and (:job-state (:system v))
                                (instance? clojure.lang.IDeref (:job-state (:system v)))
                                (= :running (:state v)))))
       (map (fn [[k v]] [k (deref (:job-state (:system v)))]))
       (into {})))
