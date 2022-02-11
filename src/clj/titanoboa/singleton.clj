; Copyright (c) Commsor Inc. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.singleton
  (:require [clojure.tools.logging :as log]))

(def singletons (atom {}))

(defn get-instance [c & args]
  (or (get @singletons c)
      (do (log/info "Instantiating a singleton bean " c)
          (get (swap! singletons assoc c (clojure.lang.Reflector/invokeConstructor c (into-array Object args)))
               c))))
