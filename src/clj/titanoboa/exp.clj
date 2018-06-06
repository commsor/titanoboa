(ns titanoboa.exp
  (:require [clojure.walk :as walk]
            [cognitect.transit :as transit]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [taoensso.nippy :as nippy]
            [titanoboa.dependencies :as deps])
  (:import [pl.joegreen.lambdaFromString LambdaFactory DynamicTypeReference]
           (java.io ObjectOutputStream)))

(def ^:dynamic *job* {})
(def ^:dynamic *properties* {})

(defrecord Expression [value type])

(defn read-expression [expr]
  (cond
    (string? expr) (->Expression expr nil)
    (vector? expr) (let [[v t] expr] (->Expression v t))
    (map? expr) (map->Expression expr)))

(def transit-write-handler (transit/write-handler (constantly "titanoboa.exp.Expression") (fn [val] [(:value val) (:type val)])))
(def transit-read-handler (transit/read-handler #(read-expression %)))

(def edn-reader-map {'titanoboa.exp.Expression #'titanoboa.exp/read-expression
                     'titanoboa.exp/Expression #'titanoboa.exp/read-expression
                     'megalodon.exp.Expression #'titanoboa.exp/read-expression
                     'megalodon.exp/Expression #'titanoboa.exp/read-expression})

(defn expression? [exp]
  (= (type exp) titanoboa.exp.Expression))

(defn- mapmap
  "Apply kf and vf to a sequence, s, and produce a map of (kf %) to (vf %)."
  ([vf s]
     (mapmap identity vf s))
  ([kf vf s]
     (zipmap (map kf s)
              (map vf s))))

(defn keys-to-lowercase [m]
  "Returns a map with all string keys turned to lowercase."
  (mapmap #(if (string? (key %)) (clojure.string/lower-case (key %)) (key %))  val m))

;;TODO spawn different namespace for each logged user (and maybe separate for each job/step/revision?) - OR USE DIFFERENT Clojure RunTime!
(defn eval-snippet [s ns-sym]
  (binding [*ns* (create-ns ns-sym)
            ] ;;TODO bind *job* and *properties* based on some test data
    (try (load-string s)
         (catch Exception e
           (log/warn "Failed to evaluate expression [" s "] - threw an Exception " e)
           e))))

(defn run-workload-fn [{:keys [step properties] :as job} &[fn-key]]
  (binding [*ns* (find-ns 'titanoboa.exp)
            *job* job
            *properties* properties]
    (try
      (let [fn-key (or fn-key :workload-fn)
            workload-fn (get step fn-key)]
        (case (:type workload-fn)
                "java" (-> (LambdaFactory/get)
                           (.createLambdaUnchecked (:value workload-fn) (DynamicTypeReference. "Function< clojure.lang.PersistentArrayMap, clojure.lang.IPersistentMap>"))
                           (.apply properties))
                ((load-string (:value workload-fn)) properties)))
      (catch Exception e
        (log/warn "workload-fn of step [" (:id step) "] threw an Exception: " e)
        {:exit-code "error"
         :error e}))))

(defn eval-quote [property]
  "Evaluates if provided job property contains expression.
	If so it evaluates it using load-string and returns result. Otherwise returns the property unchanged.
	By default all records of titanoboa.exp.Expression type are treated as potentially quoted forms.
	If (type property) is titanoboa.exp.Expression it's value (:value property) is then evaluated.
  If an Exception thrown it is logged and the Expression's value is returned withoud evaluating."
  (if (= (type property) titanoboa.exp.Expression)
    (try (load-string (:value property))
     (catch Exception e
       (log/warn "Failed to evaluate expression [" (:value property) "] - threw an Exception " e)
       (:value property)))
    property))

(defn eval-properties [properties job]
  "Traverses (depth-first) property map and evaluates all potential quoted expressions using eval-quote.
	Returns the property map with evaluated properties. As properties are evaluated in a context of give job, job also needs to be provided as a second parameter."
  (binding [*ns* (find-ns 'titanoboa.exp)
            *job* job
            *properties* (:properties job)]
    (walk/prewalk eval-quote properties)))

(defn eval-job-prop [job properties]
  "Same as eval-properties but returns directly the job map which :properties are already merged with the updated (evaluated) properties. Properties can be either a map or array of pairs."
  (let [properties (if (vector? properties) (apply hash-map properties) properties)]
    (assoc job :properties (merge (:properties job) (eval-properties properties job)))))

(defn eval-ordered [properties job]
  "Similar to eval-job-prop, but accepts also an array of maps and/or tuples and evaluates them sequentially to produce a final map.
	As properties are evaluated in a context of give job, job also needs to be provided as a second parameter.
	Returns the job map with the evaluated properties merged into jobs original properties.
	Properties types accepted:
	maps - {:maps will :be accepted :and its :properties will :B evaluated :in no :particular order}
	vector of maps - [{:will be :evaluated 1st}{:will B :evaluated 2nd}]
	vector of vectors - [[:will be :evaluated 1st][:will B :evaluated 2nd]]"
  (cond
    (map? properties) (eval-job-prop job properties)
    (vector? properties) (reduce eval-job-prop job properties)
    :else job))

(defrecord SerializedVar [symbol]) ;; TODO add node-id?

(nippy/extend-freeze clojure.lang.Var :clojure.core/var
                     [x data-output]
                     (.writeUTF data-output (str x)))

(nippy/extend-thaw :clojure.core/var
                   [data-input]
                   (SerializedVar. (.readUTF data-input)))

(nippy/extend-freeze clojure.core.async.impl.channels.ManyToManyChannel :core-async-chan
                     [x data-output]
                     nil)

(nippy/extend-thaw :core-async-chan
                   [data-input]
                   :core-async-chan)
;;FIXME cast this ino normal Exception upstream
(nippy/extend-freeze pl.joegreen.lambdaFromString.LambdaCreationException :lambda-creation-exception
                     [x data-output]
                     (.writeUTF data-output (.getMessage x)))

(nippy/extend-thaw :lambda-creation-exception
                   [data-input]
                   (java.lang.Exception. (.readUTF data-input)))

  #_(def wfn (.createLambda (LambdaFactory/get)
                        "p -> {Integer b = (Integer) p.valAt(\"a\");
                        b++;
                        return p.assoc(\"b\", b);}"
                        "Function< clojure.lang.PersistentArrayMap,  clojure.lang.IPersistentMap>"))
;;=> #'titanoboa.server/wfn
;;(.apply wfn {"a" (int 0)})
;;=> {"a" 0, "b" 1}
clojure.lang.PersistentArrayMap