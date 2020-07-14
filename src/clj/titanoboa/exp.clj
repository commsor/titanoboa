(ns titanoboa.exp
  (:require [clojure.walk :as walk]
            [cognitect.transit :as transit]
            [clojure.java.classpath :as cp]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [taoensso.nippy :as nippy]
            [titanoboa.singleton :as singleton])
  (:import [pl.joegreen.lambdaFromString LambdaFactory DynamicTypeReference LambdaFactoryConfiguration]
           (java.io ObjectOutputStream)
           (clojure.lang PersistentArrayMap)
           (java.util Map)
           (io.titanoboa.java IWorkloadFn)))

(def ^:dynamic *job* {})
(def ^:dynamic *properties* {})
(def ^:dynamic *jobdir* nil)

(def java-lambda-factory (LambdaFactory/get))

(defn init-java-lambda-factory!
  ([cl & [imports]]
   (let [c-path (reduce (fn [v i] (str v (.getCanonicalPath i) java.io.File/pathSeparatorChar)) "" (cp/classpath cl))
         config (-> (LambdaFactoryConfiguration/get)
                    (.withParentClassLoader cl)
                    (.withCompilationClassPath c-path))
         config (if imports
                  (.withImports config (into-array String imports))
                  config)]
     (alter-var-root #'java-lambda-factory (constantly (LambdaFactory/get config)))))
  ([]
   (alter-var-root #'java-lambda-factory (constantly (LambdaFactory/get)))))

(defrecord Expression [value type])

(defn read-expression [expr]
  (cond
    (string? expr) (->Expression expr nil)
    (vector? expr) (let [[v t] expr] (->Expression v t))
    (map? expr) (map->Expression expr)
    (list? expr) (->Expression expr nil)
    (symbol? expr) (->Expression expr nil)))

(def transit-write-handler (transit/write-handler (constantly "titanoboa.exp.Expression")
                                                  (fn [val]
                                                    [(:value val) (:type val)])))
(def transit-read-handler (transit/read-handler #(read-expression %)))

(def edn-reader-map {'titanoboa.exp.Expression #'titanoboa.exp/read-expression
                     'titanoboa.exp/Expression #'titanoboa.exp/read-expression
                     'megalodon.exp.Expression #'titanoboa.exp/read-expression
                     'megalodon.exp/Expression #'titanoboa.exp/read-expression})


(defn expression? [exp]
  (= (type exp) titanoboa.exp.Expression))

(defn java-expression? [exp]
  (and (expression? exp) (= (:type exp) "java")))

(defn eval-expr [^Expression expr]
  (cond
    (string? (:value expr)) (load-string (:value expr))
    (list? (:value expr)) (eval (:value expr))
    (symbol? (:value expr)) (resolve (:value expr))))

(defn eval-property [property]
  "Evaluates if provided job property contains expression.
	If so it evaluates it using load-string and returns result. Otherwise returns the property unchanged.
	By default all records of titanoboa.exp.Expression type are treated as potentially quoted forms.
	If (type property) is titanoboa.exp.Expression it's value (:value property) is then evaluated.
  If an Exception thrown it is logged and the Expression's value is returned withoud evaluating."
  (try (cond
         (expression? property) (eval-expr property)
         (list? property) (eval property)
         :else property)
       (catch Exception e
         (log/warn "Failed to evaluate expression [" property "] - threw an Exception " e)
         property)))

(defn eval-properties [properties job]
  "Traverses (depth-first) property map and evaluates all potential quoted expressions using eval-quote.
	Returns the property map with evaluated properties. As properties are evaluated in a context of give job, job also needs to be provided as a second parameter."
  (binding [*ns* (find-ns 'titanoboa.exp)
            *job* job
            *properties* (:properties job)
            *jobdir* (:jobdir job)]
    (walk/prewalk eval-property properties)))

;;TODO spawn different namespace for each logged user (and maybe separate for each job/step/revision?) - OR USE DIFFERENT Clojure RunTime!
(defn eval-snippet [s type properties ns-sym]
  (binding [*ns* (create-ns ns-sym)
            *properties* properties] ;;TODO bind *job* and *properties* based on some test data
    (try (if (= "java" type)
           (-> java-lambda-factory
               (.createLambdaUnchecked s (DynamicTypeReference. "Function< clojure.lang.PersistentArrayMap, java.util.Map>"))
               (.apply (eval-properties properties {})))
           (load-string s))
         (catch Exception e
           (log/warn "Failed to evaluate expression [" s "] - threw an Exception " e)
           e))))

(defn eval-exfn [expr]
  (cond
    (expression? expr) (eval-expr expr)
    (symbol? expr) (resolve expr)
    (list? expr) (eval expr)
    (and (class? expr) (.isAssignableFrom IWorkloadFn expr)) (singleton/get-instance expr)
    :else expr))

(defn validate-exfn [expr]
  (cond
    (fn? expr) expr
    (instance? IWorkloadFn expr) expr
    (and (class? expr) (.isAssignableFrom IWorkloadFn expr)) (singleton/get-instance expr)
    :else expr))

(defn run-exfn
  ([expr arg1]
   (cond (java-expression? expr) (do (when-not (string? (:value expr)) (throw (java.lang.IllegalStateException. "Value of java lambda expression must be String!")))
                                            (-> java-lambda-factory
                                                (.createLambdaUnchecked (:value expr) (DynamicTypeReference. "Function< clojure.lang.PersistentArrayMap, java.util.Map>"))
                                                (.apply arg1)))
         :else  (-> expr
                    eval-exfn
                    validate-exfn
                    (.invoke arg1))))
  ([expr arg1 arg2]
   (cond (java-expression? expr) (do (when-not (string? (:value expr)) (throw (java.lang.IllegalStateException. "Value of java lambda expression must be String!")))
                                            (-> java-lambda-factory
                                                (.createLambdaUnchecked (:value expr) (DynamicTypeReference. "Function< clojure.lang.PersistentArrayMap, java.util.Map>"))
                                                (.apply arg1 arg2)))
         :else  (-> expr
                    eval-exfn
                    validate-exfn
                    (.invoke arg1 arg2)))))

(defn convert-java-maps [r]
  (cond
    (map? r) r
    (instance? Map r) (PersistentArrayMap/create r)
    :else r))

(defn run-workload-fn [{:keys [step properties] :as job} &[fn-key]]
  (binding [*ns* (find-ns 'titanoboa.exp)
            *job* job
            *jobdir* (:jobdir job)
            *properties* properties]
    (try
      (let [fn-key (or fn-key :workload-fn)
            workload-fn (get step fn-key)]
        (-> (run-exfn workload-fn properties)
            convert-java-maps))
      (catch Exception e
        (log/warn "workload-fn of step [" (:id step) "] threw an Exception: " e)
        {:exit-code "error"
         :error e}))))

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
