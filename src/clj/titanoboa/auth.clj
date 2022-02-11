; Copyright (c) Commsor Inc. All rights reserved.
; The use and distribution terms for this software are covered by the
; GNU Affero General Public License v3.0 (https://www.gnu.org/licenses/#AGPL)
; which can be found in the LICENSE at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns titanoboa.auth
  (:require [honeysql.core :as sql]
            [honeysql.helpers :as h]
            [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [buddy.hashers :as hashers]
            [buddy.auth.backends.token :refer [token-backend]]
            [buddy.auth.accessrules :refer [success error]]
            [buddy.auth :refer [authenticated?]]
            [buddy.sign.jwt :as jwt]
            [buddy.core.keys :as ks]
            [ring.middleware.session :refer [wrap-session]]
            [ring.middleware.session.cookie :refer [cookie-store]]
            [clj-time.core :as t]
            [clojure.java.io :as io]))


(defn create-user [ds user]
  (->
  (jdbc/insert! ds "users" (-> user
                               (assoc :password_digest (hashers/encrypt (:password user)))
                               (dissoc :password)))
  first
  (dissoc :password_digest)))

(defn- find-user-by [ds field value]
  (some->> (-> (h/select :*)
               (h/from :users)
               (h/where [:= field value]))
           (sql/format)
           (jdbc/query ds )
           first))

(defn- get-user [ds name]
  (find-user-by ds :name name))

(defn password-matches?
  [ds name password]
  (some-> (h/select :password_digest)
          (h/from :users)
          (h/where [:= :name name])
          sql/format
          (jdbc/query ds)
          first
          :password_digest
          (->> (hashers/check password))))

(defn auth-user [ds name password]
  (let [user (get-user ds name)
        unauthed [false {:message "Invalid username or password"}]]
    (if user
      (if (hashers/check password (:password_digest user))
        [true {:user (dissoc user :password_digest)}]
        unauthed)
      unauthed)))

(defn- pkey [auth-conf]
  (ks/private-key
   (:privkey auth-conf)
   (:passphrase auth-conf)))

(defn create-auth-token [ds auth-conf name password]
  (let [[ok? res] (auth-user ds name password)
        exp (t/plus (t/now) (t/days 1))]
    (if ok?
      [true {:token (jwt/sign res
                              (pkey auth-conf)
                              {:alg :rs256 :exp exp})}]
      [false res])))

(defn wrap-auth-cookie [handler cookie-secret]
  (-> handler
      (wrap-session
        {:store (cookie-store {:key cookie-secret})
         :cookie-name "id"
         :cookie-attrs {:max-age (* 60 60 24)}})))

(defn unsign-token [token pubkey]
  (jwt/unsign token pubkey {:alg :rs256}))


(defn wrap-auth-token [handler {:keys [pubkey extensions] :as auth-conf}]
  (let [public-key (ks/public-key pubkey)]
    (fn [req]
      (let [user (:user (when-let [token (-> req :session :token)]
                          (unsign-token token public-key)))]
        (if (and (not user) (contains? extensions (:uri req)))
          (do
            (log/info "Security extension applicable for URI " (:uri req))
            (handler ((get extensions (:uri req)) req)))
          (handler (assoc req :auth-user user)))))))

(defn wrap-authentication [handler]
  (fn [req]
    (if (:auth-user req)
      (handler req)
      {:status 401})))