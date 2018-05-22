(ns user
  (:require [figwheel-sidecar.repl-api :as ra]))
(ra/start-figwheel!) ;; <-- fetches configuration
(ra/cljs-repl)