(ns titanoboa.handler
  (:require [compojure.core :refer [GET POST PATCH PUT defroutes routes context]]
            [compojure.route :as route :refer [not-found resources]]
            [compojure.coercions :refer [as-int]]
            [clj-http.client :as client :refer [request]]
            [clj-http.util :as http-util]
            [ring.middleware.format :refer [wrap-restful-format]]
            [ring.middleware.format-params :refer [wrap-restful-params]]
            [ring.middleware.format-response :refer [wrap-restful-response]]
            [ring.middleware.reload :refer [wrap-reload]]
            [ring.middleware.params :refer [wrap-params]]
            [me.raynes.fs :as fs]
            [clojure.java.io :as io]
            [hiccup.core :refer [html]]
            [hiccup.page :refer [include-js include-css]]
            [titanoboa.util :as util]
            [titanoboa.repo :as repo :refer [keyify]]
            [titanoboa.exp :as exp]
            [titanoboa.processor :as processor]
            [titanoboa.system :as system]
            [titanoboa.api :as api]
            [titanoboa.channel :as channel]
            [titanoboa.dependencies :as deps]
            [cognitect.transit :as transit]
            [clojure.tools.logging :as log]
            [titanoboa.auth :as auth]
            [titanoboa.database :as db]
            [ring.util.response :as resp]
            [compliment.core :as compliment])
  (:import [java.net URI]
           [com.mchange.v2.c3p0 ComboPooledDataSource]))

(defn read-n-lines [n filename]
  (with-open [rdr (io/reader filename)]
    (doall (take-last n (line-seq rdr)))))

;;TODO consider instead of passing config as tehis fnction's parameter injecting necessary config directly into request via additional ring middleware?
(defn get-secured-routes [{:keys [steps-repo-path jobs-repo-path systems-catalogue archive-ds-ks node-id log-file-path] :or {log-file-path "titanoboa.log"} :as config}]
  (routes
    (GET "/user" req {:body (:auth-user req)})
    (POST "/user/logout" req {:status 401 :session {}})
    (GET "/log" [lines :<< as-int] {:body (when (and log-file-path (fs/exists? log-file-path))
                                                (vec (read-n-lines lines log-file-path)))})
    (context "/repo/stepdefinitions" [] ;;TODO following should be loaded from atom which would be asynchronously updated from file system on change
      (GET "/" [] {:body (repo/get-all-head-defs steps-repo-path)})
      (GET "/heads" [] {:body (into {} (repo/list-head-defs steps-repo-path))})
      (GET "/revisions" [] {:body (into {} (repo/list-all-revisions steps-repo-path))})
      (GET "/:def-name" [def-name] {:body (repo/get-head-def steps-repo-path def-name)})
      (GET "/:def-name/:revision" [def-name revision] {:body (repo/get-def-rev steps-repo-path def-name (Integer. revision))})
      (POST "/:def-name" [step-type definition notes :as r] {:body (repo/save! {:def (assoc definition :type (util/tokey step-type))
                                                                                :repo-path steps-repo-path
                                                                                :key :type
                                                                                :user (or (get-in r [:auth-user :name]) "anonymous")
                                                                                :revision-notes (or notes "")})}))
    (context "/repo/jobdefinitions" [] ;;TODO following should be loaded from atom which would be asynchronously updated from file system on change
      (GET "/" [] {:body (keyify :name (repo/get-all-head-defs jobs-repo-path))})
      (GET "/heads" [] {:body (into {} (repo/list-head-defs jobs-repo-path))})
      (GET "/revisions" [] {:body (into {} (repo/list-all-revisions jobs-repo-path))})
      (GET "/:def-name" [def-name] (if-let [jd (repo/get-head-def jobs-repo-path def-name)]
                                     {:body jd}
                                     {:status 404 :body {:status "error" :message (str "Job definition " def-name " was not found!")}}))
      (GET "/:def-name/:revision" [def-name revision] (if-let [jd-revision (repo/get-def-rev jobs-repo-path def-name (Integer. revision))]
                                                        {:body jd-revision}
                                                        {:status 404 :body {:status "error" :message (str "Revision " revision " was not found!")}}))
      (POST "/:def-name" [def-name definition notes :as r] {:body (repo/save! {:def (assoc definition :name def-name)
                                                                         :repo-path jobs-repo-path
                                                                         :user (or (get-in r [:auth-user :name]) "anonymous")
                                                                         :revision-notes (or notes "")})
                                                            :status 201})
      (POST "/:def-name/repl" [def-name snippet type properties] {:body {:result (exp/eval-snippet snippet type properties 'titanoboa.exp)}})
      (POST "/:def-name/autocomplete" [def-name snippet] {:body {:result (->>(compliment/completions snippet {:ns 'titanoboa.exp})
                                                                             (mapv (fn [{:keys [candidate]}]
                                                                                     candidate))
                                                                             (take 50))}}))
    (context "/systems" []
      (GET "/" [] {:body (merge-with merge (into {} (system/live-systems)) systems-catalogue)})
      (GET "/live" [] {:body (system/live-systems)})
      (GET "/catalogue" [] {:body systems-catalogue})
      (GET "/jobs" [] {:body (api/get-jobs-states)}))
    (context "/systems/:system" [system]
      (PATCH "/" [action wcount scope]
        {:body
         (case action
           :stop (do (system/stop-system! (util/s->key (http-util/url-decode system))) {:status 200})
           ;;:start (system/start-system! (util/tokey system) systems-catalogue config)
           :start (do (system/start-system-bundle! (util/s->key (http-util/url-decode system)) systems-catalogue config (int (or wcount 0)) scope) {:status 200})
           :restart {:status 200 :body (system/restart-system! (util/s->key (http-util/url-decode system)) systems-catalogue config)})})
      (POST "/workers" [] {:body (system/start-workers! (util/s->key (http-util/url-decode system)) systems-catalogue 1)});; TODO add PATCH to stop/start workers?
      (POST "/jobs" [sync & conf] (do (log/debug "Recieved request to start a job on system [" (http-util/url-decode system) "] with config ["conf"]")
                               {:status 201 :body (processor/run-job! (util/s->key (http-util/url-decode system)) conf sync)}))
      (POST "/jobs/:jobdef-name" [jobdef-name & properties] (do (log/debug "Recieved request to start a job " jobdef-name " on system [" (http-util/url-decode system) "] with properties "properties)
                                      {:status 201 :body (processor/run-job! (util/s->key (http-util/url-decode system)) {:jobdef-name jobdef-name :properties properties} false)}))
      (POST "/jobs/:jobdef-name/:revision" [jobdef-name revision & properties] (do (log/debug "Recieved request to start a job " jobdef-name " on system [" (http-util/url-decode system) "] with properties "properties)
                                                                {:status 201 :body (processor/run-job! (util/s->key (http-util/url-decode system)) {:jobdef-name jobdef-name :revision revision :properties properties} false)})))
    (context "/cluster" []
      (GET "/" []  {:status 404 :body {:message "Clustering is disabled"}})
      (GET "/id" [] {:status 404 :body {:message "Clustering is disabled"}})
      (GET "/jobs" [] {:body (api/get-jobs-states)})
      (GET "/dependencies" [] (if (deps/get-deps-path-property)
                                {:status 200 :body {:dependencies (deps/get-deps-file-content)}}
                                {:status 404 :body {:result "dependencies path was not set"}}))
      (PATCH "/dependencies" [old-content new-content] (if (deps/write-deps-file! old-content new-content)
                                                         {:status 200 :body {:result :ok}}
                                                         {:status 409 :body {:result :stale}})))
    (context "/cluster/nodes" []
      (GET "/" [] {:body
                     {node-id
                       {:systems (merge-with merge (into {} (system/live-systems)) systems-catalogue) :last-hearbeat-age 0 :source true :state :live}}}))
    (context "/archive" []
      (GET "/jobs" [limit :<< as-int offset :<< as-int order-by order] (do (log/info "Received request to list jobs, limit is ["limit"] order is " order)
                                                                  (if archive-ds-ks
                                                                    {:body (db/list-jobs (get-in @system/systems-state archive-ds-ks) (or limit 50) (or offset 0) (when (and order-by order) [(keyword order-by) (keyword order)]))}
                                                                    {:status 404 :body {}})))
      (GET "/jobs/:jobid" [jobid] (if archive-ds-ks  {:body (db/get-job (get-in @system/systems-state archive-ds-ks) jobid)}
                                                     {:status 404 :body {}})))))

(defn get-public-routes [{:keys [auth-ds-ks auth-conf auth?] :as config}]
  (routes
    (route/resources "/")
    (GET "/" [] (resp/content-type (resp/resource-response "index.html" {:root "public"}) "text/html"))
    (POST "/create-auth-token" [name password] (if (and auth? auth-ds-ks auth-conf)
                                                 (let [[ok? res] (auth/create-auth-token (get-in @system/systems-state auth-ds-ks)
                                                                                              auth-conf name password)]
                                                        (if ok?
                                                          {:status 201 :body (assoc res :name name) :session {:token (:token res)}}
                                                          {:status 401 :body res}))
                                                 {:status 404 :body {}}))))

(def *req (atom nil))

(defn simple-logging-middleware [handler]
  (fn [request]
    (log/debug "Retrieved Http request:" request " with following params: " (:params request))
    #_(reset! *req request);;FIXME comment out this line for production use!
    ;;    (log/info (:params request))
    (handler request)))

(defn fallback-exception-middleware
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        (log/error e)
        {:status 500 :body {:message (str "Something isn't quite right... \n " e)}}))))


(defn get-app-routes [{:keys [auth? auth-conf] :as config}]
  (if auth?
    (routes
      (-> (get-public-routes config)
          simple-logging-middleware
          #_(auth/wrap-auth-token auth-conf)
          simple-logging-middleware
          fallback-exception-middleware)
      (-> (get-secured-routes config)
          auth/wrap-authentication
          simple-logging-middleware
          fallback-exception-middleware
          (auth/wrap-auth-token auth-conf)))
    (routes
      (-> (get-public-routes config)
          simple-logging-middleware
          fallback-exception-middleware)
      (-> (get-secured-routes config)
          simple-logging-middleware
          fallback-exception-middleware))))

(defn prepare-cookies
  "Removes the domain and secure keys from cookies map.
  Also converts the expires date to a string in the ring response map."
  [resp]
  (let [prepare #(-> (update-in % [1 :expires] str)
                     (update-in [1] dissoc :domain :secure))]
    (assoc resp :cookies (into {} (map prepare (:cookies resp))))))

(defn slurp-binary
  "Reads len bytes from InputStream is and returns a byte array."
  [^java.io.InputStream is len]
  (with-open [rdr is]
    (let [buf (byte-array len)]
      (.read rdr buf)
      buf)))

(def transit-handlers-encode {titanoboa.exp.Expression exp/transit-write-handler
                              clojure.lang.Var (transit/write-handler (constantly "s") #(str %))
                              titanoboa.channel.SerializedVar (transit/write-handler (constantly "s") #(:symbol %))
                              java.util.GregorianCalendar (transit/write-handler (constantly "m") #(.getTimeInMillis %) #(str (.getTimeInMillis %)))
                              org.joda.time.DateTime (transit/write-handler (constantly "m") #(-> % .toDate .getTime) #(-> % .toDate .getTime .toString))
                              java.io.File (transit/write-handler (constantly "s") #(.getCanonicalPath %))
                              java.lang.Exception (transit/write-handler (constantly "s") #(str %)) ;;FIXME - properly serialize stack trace etc.
                              clojure.lang.Fn (transit/write-handler (constantly "s") #(str %))
                              clojure.lang.Atom (transit/write-handler (constantly "s") #(str %))
                              clojure.core.async.impl.channels.ManyToManyChannel (transit/write-handler (constantly "s") #(str %))})

(def transit-handlers-decode {"titanoboa.exp.Expression" exp/transit-read-handler})

;;JSON encoders/decoders
(cheshire.generate/add-encoder java.io.File
                               (fn [f jsonGenerator]
                                 (.writeString jsonGenerator (.getCanonicalPath f))))

(cheshire.generate/add-encoder java.lang.Object
                               (fn [o jsonGenerator]
                                 (.writeString jsonGenerator (.toString o))))

;;TODO handle java.lang.Object
(defn get-ring-app [config]
  (-> (get-app-routes config)
      ;;simple-logging-middleware
      (wrap-restful-format {:formats [:transit-json :transit-msgpack :edn :json-kw]
                            :response-options {:transit-json
                                                {:handlers transit-handlers-encode
                                                 :default-handler (transit/write-handler (constantly "s") #(str %))}}
                            :params-options {:transit-json
                                                {:handlers transit-handlers-decode}}})
      wrap-params
      (auth/wrap-auth-cookie "SoSecret12345678")))

