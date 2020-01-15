(ns titanoboa.processor
  (:require [clojure.core.async :as async :refer [go-loop go >! <! >!! <!! thread chan timeout onto-chan dropping-buffer]]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [clojure.walk :as walk]
            [clojure.set :refer [subset?]]
            [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [titanoboa.util :as util]
            [titanoboa.system :as system]
            [titanoboa.exp :as exp]
            [titanoboa.channel :as channel]
            [titanoboa.util :as util :refer [store-file]])
  (:import (java.io FileOutputStream DataInputStream DataOutputStream)))

(def ^:dynamic *cmd-exchange-ch* nil)
(def ^:dynamic *server-config* nil)

(defn preprocess [job]
  (log/info "Entering preprocessing method...")
  (print "preprocessing...."))

;;TODO move this to a separate namespace "commands" or similar?
(defn instantiate-job! [{:keys [tracking-id id jobdef jobdef-name revision properties files new-jobs-ch state-agent job-folder defs-atom mq-pool callback-ch] :as config}]
  (let [id (or id (str (java.util.UUID/randomUUID)))
        jobdir (java.io.File. job-folder id)
        _ (assert (or jobdef (and defs-atom jobdef-name)))
        revision (if revision (Integer/parseInt revision) nil)
        jobdef (or jobdef (get-in @defs-atom [jobdef-name (or revision :head) :job-def]))
        jobdef-name (or (:name jobdef) jobdef-name)
        create-folder? (not (false? (get-in jobdef [:properties :create-folder?])))
        properties (if create-folder? (merge properties {:jobdir jobdir "jobdir" jobdir}) properties)
        init-job {:jobid id
                  :tracking-id tracking-id
                  :step-retries {}
                  :jobdef jobdef
                  :create-folder? create-folder?
                  :jobdir jobdir
                  :state :initial
                  :step nil
                  :next-step nil
                  :start (java.util.Date.)
                  :history []
                  :commands-ch nil
                  :callback-ch callback-ch}
        job (->> ;;TODO rewrite this to use cond->> to call these functions only if not null
              (exp/eval-ordered (:properties jobdef) init-job);;eval initial properties
              (exp/eval-ordered properties));;override properties if there are any
        create-folder? (:create-folder? job) ]

    (assert jobdef (str "Job definition [" jobdef-name "] does not exist!"))
    ;;create job working directory - in the dosync - use an agent? - nope this should be safe to retry
    (if (or create-folder? files)
      (do
        (log/info "Creating job directory for job [" id "]...")
        (.mkdir jobdir)))

    ;;store files included (if any) to the working directory
    ;;TODO - is this safe to retry? if not use Agent
    (if files
      (doseq [[k v] files]
        (log/debug "Storing file " (name k))
        (store-file jobdir (name k) v)))
    ;;update ref with running jobs map
    (send state-agent assoc id job)
    ;;TODO insert into the state machine pipeline - do it in the transaction? (agent would be required)
    (log/info "Submitting new job [" job "] into new jobs channel...")
    ;;TODO also if the job is created to be in "suspended" state it will never be put into new-jobs-ch, but into some "paused jobs" map (or maybe keeping it in the running-jobs ref is sufficient?)
    (if (and (channel/distributed? new-jobs-ch) mq-pool)
      (channel/with-mq-sessionpool mq-pool
                                   (>!! new-jobs-ch job))
      (>!! new-jobs-ch job))
    (if tracking-id
      [tracking-id id]
      id)))

;;TODO move this to a separate namespace (maybe to system?)?
(defn run-sync-job! [{:keys [new-jobs-ch mq-pool] :as config}]
  "Initiates a job and synchronously blocks until it finishes. Returns the job map once the job finished.
  Waiting for the job's result is done via registering a core.async channel as a callback channel.
  If the system is distributed (i.e. service bus used is e.g. RabbitMQ as opposed to core.async channels)
  than a temporary MQ queue is created for the callback - via the channel/->distributed-ch function."
  (let [callback-ch (async/chan 1)]
    (if (and (channel/distributed? new-jobs-ch) mq-pool)
      (channel/with-mq-sessionpool mq-pool
          (instantiate-job! (assoc config :sync true
                              :callback-ch (channel/->distributed-ch callback-ch))))
      (instantiate-job! (assoc config :sync true :callback-ch callback-ch)))
    (<!! callback-ch)))

;;TODO move this to a separate namespace (maybe to system?)?
;;FIXME fix this need to go backdoor to grab systems config - it should flow down from the top!!! This is likely caused by my use of actions and action pool - functions from processor make calls to fns in the same namespace via actions, this seems weird!!
;;FIXME be consistent about jobdef-id vs jobdef-name
;;TODO add revision handling
(defn start-job!
  "Dispatches an action request to start a job in given system. The request is dispatched to the system's action thread pool.
  If a response channel is provided (it has to be a core async channel, distributed channels are not accepted!) the job's id will be put on the channel once the job has been instantiated.
  If the response channel is not provided, the function waits synchronously for the job to be instantiated and then returns the job id."
  ([system-key {:keys [jobdef jobdef-name revision properties files] :as conf} response-ch keep-open]
  (assert (system/is-running? system-key) "Cannot start a job in a system that is not running!")
  (let [{:keys [action-chan new-jobs-chan job-state job-folder-root job-defs mq-session-pool] :as system} (get-in @system/systems-state [system-key :system]) ;;FIXME fix this need to go backdoor to grab systems config - it should flow down from the top!!! This is likely caused by my use of actions and action pool - functions from processor make calls to fns in the same namespace via actions, this seems weird!!
        action-request {:action-fn titanoboa.processor/instantiate-job!
                        :data [(merge conf
                                      {:new-jobs-ch new-jobs-chan
                                       :state-agent job-state
                                       :job-folder job-folder-root
                                       :defs-atom job-defs
                                       :mq-pool (:pool mq-session-pool)})]
                        :response-ch response-ch
                        :keep-open keep-open}]
    (when-not (>!! action-chan action-request)
      (throw (IllegalStateException. (str "Action channel on system" system-key "is not open!"))))))
  ([system-key {:keys [jobdef jobdef-name properties files] :as conf}]
   (let [response-ch (chan 1)]
     (start-job! system-key conf response-ch false)
     (<!! response-ch))))

(defn run-job!
  "Dispatches a request to start a job in given system. Does not use action thread pool.
  Returns the job id or the finished job if the sync flag is set to true."
  [system-key {:keys [jobdef jobdef-name revision properties files] :as conf} sync]
  (assert (system/is-running? system-key) "Cannot start a job in a system that is not running!")
  (let [{:keys [new-jobs-chan job-state job-folder-root job-defs mq-session-pool] :as system} (get-in @system/systems-state [system-key :system])
        job-conf (merge conf {:new-jobs-ch new-jobs-chan
                              :state-agent job-state
                              :job-folder job-folder-root
                              :defs-atom job-defs
                              :mq-pool (:pool mq-session-pool)})] ;;FIXME fix this need to go backdoor to grab systems config - it should flow down from the top!!! This is likely caused by my use of actions and action pool - functions from processor make calls to fns in the same namespace via actions, this seems weird!!
    (if sync
      (run-sync-job! job-conf)
      {:jobid (instantiate-job! job-conf)})))

(defn init-first-step [{{:keys [steps] :as jobdef} :jobdef :keys [state next-step jobdir properties step-start] :as job}]
  "Initializes first step of a job. Takes a job map as an argument and returns the updated map. The step function is NOT called in the process.
	if provided job is in :initial state, it finds a first step in steps vector and uses it as a current step.
	Job's properties are updated with step properties (and overriden if necessary) and its state is changed to :running."
  (if (= state :initial)
    (let [step (first steps)]
      (log/info "Initializing a new job; First step will be: [" (:id step) "]")
      (assoc job
             :step step
             :state :running
             :step-start step-start))
    job))

(defn get-step [{{:keys [steps] :as jobdef} :jobdef :as job} step-id]
  (first (filter #(= (:id %) step-id) steps)))

(defn next-step-join? [job next-step-id]
  (if-not (vector? next-step-id)
    (-> (get-step job next-step-id)
        :supertype
        (= :join))
    (if (empty? (filterv #(-> (get-step job %)
                  :supertype
                  (= :join)) next-step-id))
      false
      (throw (IllegalStateException. "Multiple next steps found, but one of them is join! Join can be used only as a single next step.")))))

(defn init-step [{{:keys [steps] :as jobdef} :jobdef :keys [state step-start next-step jobdir properties step-retries node-id] :as job}]
  (let [step (first (filter #(= (:id %) next-step) steps))
        retry-count (or (get step-retries next-step) 0)
        retrying? (> retry-count 0)]
      (log/info "Initializing a next step; next step ["next-step"] was found among steps as step [" (:id step) "]")
      (assoc job
             :step step :step-start step-start :step-state (if retrying? :retrying :running) :history (conj (get job :history) {:id (:id step) :step-state (if retrying? :retrying :running) :start step-start :node-id node-id :retry-count retry-count}))))

(defn initialize-step [{:keys [state] :as job}]
  (if (= state :initial)
    (init-first-step job)
    (init-step job)))

(defn normalize-result [step-result]
  (if (string? step-result) (clojure.string/lower-case step-result) step-result))

#_(defn find-next-step [current-step step-result]
  "Finds the next step from the step's ':next' map based on the result of the current step.
  String values are compared irrespective of case. If no explicit mapping for the result is found and a wildcard '*' is present, its value is used.
  Wildcards are not applied in case of error (i.e. when result equals 'error')."
  (let [next-map (:next current-step)
        normalized-map (exp/keys-to-lowercase next-map)
        normalized-result (normalize-result step-result)]
  (if (= normalized-result "error")
      (get normalized-map normalized-result)
      (get normalized-map normalized-result (get normalized-map "*")))))

;;TODO add support for expressions - jast pas on steps to eval-properties fn
(defn find-matching-steps [steps r]
  (let [filter-fn (if (= r "error")
                    #(= r (first %))
                    #(or (= r (first %))
                         (= "*" (first %))
                         (= :* (first %))
                         #_(and (exp/expression? (first %))
                                (true? (exp/eval-property (first %))))))]
    (->> steps
      (filter filter-fn)
      (mapv second))))

(defn find-next-step [current-step step-result]
  "Finds the next step from the step's ':next' map based on the result of the current step.
  String values are compared irrespective of case. If no explicit mapping for the result is found and a wildcard '*' is present, its value is used.
  Wildcards are not applied in case of error (i.e. when result equals 'error')."
  (let [next-steps (:next current-step)
        multiple? (:allow-parallel? current-step)
        next-steps (if (map? next-steps) (into [] next-steps) next-steps)
        normalized-steps (mapv #(if (string? (first %)) [(clojure.string/lower-case (first %)) (second %)] %) next-steps)
        normalized-result (normalize-result step-result)
        matching-steps (find-matching-steps normalized-steps normalized-result)]
    (if (or (not multiple?) (<= (count matching-steps) 1))
      (first matching-steps)
      matching-steps)))

(defn process-map-step [{{:keys [jobdef-name sys-key standalone-system? distributed-system? workers-count aggregator-q-name create-folder?] :as properties} :properties
                         {step-id :id}                                                                                                  :step map-steps :map-steps jobdir :jobdir jobid :jobid :as job}]
  (assert jobdir)
  (when standalone-system? ;; TODO add also implement check for distributed-system? - whether or not start the system only on the current node or on all (or potentially some) nodes; alternatively this could be retrieved from the system catalogue (e.g. assumed async.core based system is inherently NOT distributed)
    (channel/with-mq-session (channel/current-session)
                             (async/>!! *cmd-exchange-ch*
                                        `(titanoboa.system/start-system-bundle! ~sys-key (:systems-catalogue ~'titanoboa.server/server-config) ~'titanoboa.server/server-config ~workers-count ~jobid)))
    (system/start-system-bundle! sys-key (:systems-catalogue *server-config*) *server-config* workers-count jobid))
  (let [sys-key (if standalone-system? (system/scope->key sys-key jobid) sys-key)
        result-seq (exp/run-workload-fn job)
        response-ch (chan 1024)
        commit-log (java.io.File. jobdir (str step-id ".map.log"))
        failover? (.exists commit-log)
        processed-id-tuples (if-not failover? (atom []) (atom (read-string (str "[" (slurp commit-log) "]"))))
        processed-indexes (if-not failover?  (atom #{}) (atom (set (map first @processed-id-tuples))))
        #_(when-not failover? (.createNewFile commit-log))]
    (if-not (:error result-seq)
      (do
        (thread
          (log/info "Initiating an splitter commit log thread...")
          (with-open [writer (clojure.java.io/writer commit-log :append true)]
            (loop [[idx id :as tuple] (async/<!! response-ch)]
              (when tuple
                (.write writer (str tuple))
                (.flush writer)
                (swap! processed-id-tuples conj tuple)
                (swap! processed-indexes conj idx)
                (recur (async/<!! response-ch))))))
        (let [aggregator-callback-ch (channel/mq-chan aggregator-q-name false)
              _ (log/info "Instantiated aggregator-callback channel " aggregator-callback-ch)
              dispatched-indexes (-> (map-indexed (fn [idx i] ;;TODO for large number of jobs make this multithreaded
                                                    (when-not (contains? @processed-indexes idx)
                                                      (start-job! sys-key (merge {:jobdef-name jobdef-name :tracking-id idx :callback-ch aggregator-callback-ch} i) response-ch true))
                                                    idx)
                                                  result-seq)
                                     set)
              aggregator-notif-ch (channel/mq-chan nil false)
              _ (log/info "Finished dispatching requests for new sub jobs in an splitter step [" step-id "] of a job [" jobid "]. In total dispatched [" (count dispatched-indexes) "] requests. Note that these sub jobs may not be committed yet.")
              commit-callback-ch (thread
                                   (loop []
                                     (if (= @processed-indexes dispatched-indexes)
                                       (do (log/info "Finished instantiating new sub jobs in an splitter step [" step-id "] of a job [" jobid "]. In total instantiated [" (count @processed-indexes) "] jobs. Closing commit log now...")
                                           (async/close! response-ch)
                                           @processed-id-tuples)
                                       (do (Thread/sleep 50)
                                           (recur)))))]
          {:exit :ok :commit-callback-ch commit-callback-ch :aggregator-notif-ch aggregator-notif-ch
           :map-step {:dispatched-indexes dispatched-indexes :aggregator-notif-ch aggregator-notif-ch :aggregator-callback-ch aggregator-callback-ch :sys-key sys-key :standalone-system? standalone-system?}}))
      result-seq)))

(defn process-reduce-step [{{:keys [terminate-standalone? map-step-id commit-interval] :as properties} :properties
                         {step-id :id :as step} :step map-steps :map-steps jobdir :jobdir jobid :jobid :as job}]
  (assert jobdir) ;;TODO use try/catch?
    (let [map-step (get map-steps map-step-id)
          _ (assert map-step "No matching map step found for a reduce step." )
          {:keys [dispatched-indexes aggregator-notif-ch aggregator-callback-ch standalone-system? sys-key]} map-step
          dispatched-count (count dispatched-indexes)
          workload-fn (get step :workload-fn)
          map-step-id-log (java.io.File. jobdir (str step-id ".reduce.notif"))
          map-step-id-tuples (if-not (.exists map-step-id-log) (atom nil) (atom (read-string (slurp map-step-id-log))))
          processed-indexes (atom #{})
          processed-count (atom 0)
          uncommitted-msgs (atom [])
          commit-log-tuples (java.io.File. jobdir (str step-id ".reduce.log"))
          processed-tuples (if-not (.exists commit-log-tuples) (atom [])
                                                               (atom (read-string (str "[" (slurp commit-log-tuples) "]")))) ;;FIXME read only if non zero size!!!
           #_(when-not (.exists commit-log-tuples) (.createNewFile commit-log-tuples))
          commit-log (java.io.File. jobdir (str step-id ".reduce.result"))
          commit-log-bkp (java.io.File. jobdir (str step-id ".reduce.result.bkp"))
          failover? (.exists commit-log)
          result (if-not failover? nil
                                   (try
                                     (with-open [in (DataInputStream. (io/input-stream commit-log))] ;;FIXME read only if non zero size!!!
                                         (nippy/thaw-from-in! in))
                                     (catch Exception e
                                       (log/warn "Failed to read from commit log of a reduce step " step-id "! Trying to read redundant log file...")
                                       (with-open [in (DataInputStream. (io/input-stream commit-log-bkp))] ;;FIXME read only if non zero size!!!
                                         (nippy/thaw-from-in! in)))))
           #_(when-not failover? (.createNewFile commit-log)
                                (.createNewFile commit-log-bkp))
          commit-fn (fn [r] ;;TODO keep the output streams opened to improve performance (close them when finished and on exception) - i.e. take the stream as an input argument; move the with open macro out and embed the whole loop in it
                      (with-open [w (DataOutputStream. (io/output-stream commit-log))]
                        (nippy/freeze-to-out! w r)
                          (.flush w))
                      (with-open [writer (clojure.java.io/writer commit-log-tuples :append true)]
                        (doall (map #(.write writer (str [(:tracking-id %) (:jobid %)])) @uncommitted-msgs))
                        (.flush writer))
                      (doall (map #(channel/ack! aggregator-callback-ch %) @uncommitted-msgs))
                      (with-open [w (DataOutputStream. (io/output-stream commit-log-bkp))]
                        (nippy/freeze-to-out! w r)
                        (.flush w))
                      (reset! uncommitted-msgs []))
      _ (log/debug "Reduce step " step-id " is starting to poll following aggregator-callback channel: [" aggregator-callback-ch "]. Awaiting [" dispatched-count "] dispatched messages...")
      _ (log/debug "Map step is " map-step)
      end-result (loop [result result]
        (channel/alt!! [aggregator-callback-ch aggregator-notif-ch]
                       ([m ch]
                         (cond
                           (= ch aggregator-callback-ch) (let [r (exp/run-exfn workload-fn result (:properties m))] ;;
                                                           (log/debug "Aggregator step " step-id " retrieved a callback message from a splitter step: " m)
                                                             (swap! processed-count inc) ;;TODO currently this is now single-threaded but if multithreaded in future use STM!
                                                             (swap! processed-indexes conj (:tracking-id m))
                                                             (swap! processed-tuples conj [(:tracking-id m) (:jobid m)])
                                                             (swap! uncommitted-msgs conj m)
                                                             (when-not (< (count @uncommitted-msgs) commit-interval)
                                                               (commit-fn r))
                                                             (if (and (>= @processed-count dispatched-count) @map-step-id-tuples (subset? (set @map-step-id-tuples) (set @processed-tuples)))
                                                               (do (log/debug "NOT RECURRING; sets are " (set @map-step-id-tuples) (set @processed-tuples))
                                                                   (when-not (empty? @uncommitted-msgs) (commit-fn r))
                                                                   (channel/ack! ch @map-step-id-tuples)
                                                                    r)
                                                               (do (log/debug "RECURRING; sets are " (set @map-step-id-tuples) (set @processed-tuples))
                                                                   (recur r))))
                           (= ch aggregator-notif-ch) (do (log/debug "Aggregator step " step-id " retrieved a notification message from a splitter step: " m)
                                                          (reset! map-step-id-tuples m)
                                                          (spit map-step-id-log m)
                                                          (if (and (>= @processed-count dispatched-count) (subset? (set @map-step-id-tuples) (set @processed-tuples)))
                                                            (do (log/debug "NOT RECURRING ; sets are " (set @map-step-id-tuples) (set @processed-tuples))
                                                                (when-not (empty? @uncommitted-msgs) (commit-fn result))
                                                                (channel/ack! ch @map-step-id-tuples)
                                                                result)
                                                            (do (log/debug "RECURRING ; sets are " (set @map-step-id-tuples) (set @processed-tuples))
                                                                (recur result))))
                           :else (throw (IllegalStateException. "Unexpected channel responded to blocking alt!!"))))
                       :priority true))]
      (thread
        (channel/delete-mq-chan aggregator-callback-ch)
        (channel/delete-mq-chan aggregator-notif-ch)
        (when (and standalone-system? terminate-standalone? sys-key)
          (channel/with-mq-session (channel/current-session)
                                 (async/>!! *cmd-exchange-ch*
                                            `(titanoboa.system/stop-system! ~sys-key ~jobid)))
          (let [stopped-system (system/stop-system! sys-key jobid)]
            (Thread/sleep 1000)
            (system/cleanup-system! stopped-system))))
      {:result end-result :reduce-step {:map-step-id map-step-id :map-step-id-tuples @map-step-id-tuples}})
    ;;TODO remove given map-step-id from the map-steps map and pass it on? - so as same map step cannot be processed by two separate reduce steps?
  ;;FIXME test if works when used on undistributed system with async channels!!!
)

(defn get-thread-id [{:keys [thread-stack] :as job}]
  (some-> thread-stack
          last
          second))

(defn dispatch4join? [{:keys [thread-stack] :as job}]
  (and thread-stack
       (not-empty thread-stack)
       (not= (get-thread-id job) :main)))

(defn orchestrate-join? [{:keys [thread-stack] :as job}]
  (and thread-stack
       (not-empty thread-stack)
       (= (get-thread-id job) :main)))

(defn assess4retry [{:keys [retry-on-error? max-retries id] :as step} step-retries-map]
  (let [retry-count (or (get step-retries-map id) 0)]
    (if (and retry-on-error? (> max-retries retry-count))
      [true (assoc step-retries-map id (inc retry-count))]
      [false step-retries-map])))

(defn contains-some
  "Returns the first non-nil value of (pred x) for any x in coll,
  else nil.  One common idiom is to use a set as pred, for example
  this will return :fred if :fred is in the sequence, otherwise nil:
  (some #{:fred} coll)"
  [pred coll]
  (when (seq coll)
    (if-not (nil? (pred (first coll))) (pred (first coll)) (recur pred (next coll)))))

(defn process-step [{:keys [state step step-start start map-steps reduce-steps thread-stack step-retries] :as job} node-id]
  "Processes current step by evaluating and calling step's workload function. Returns a touple of commit callback channel (if applicable) and the updated step map.
  The commit callback channel is used only if one of the step's threads is still running upon steps completion
  - the channel will be used to defer current job message's receipt acknowledgement."
  (let [step-id (:id step)
        retry-count (get step-retries step-id)
        message-start (str "Step [" step-id "] in progress...\n")
        _ (log/debug message-start)
        result-map (case (:supertype step)
                     (:tasklet :join) (exp/run-workload-fn job)
                     :map (process-map-step job)
                     :reduce (process-reduce-step job)
                     (throw (IllegalStateException. "Invalid supertype for a step.")))
        result (if (map? result-map);;if workload-fn doesnt return map assume it just returned exit code
                 (contains-some result-map #{:exit :code :return :return-code :exit-code})
                 result-map)
        returned-props (when (map? result-map)
                         (if (contains? result-map :properties)
                           (:properties result-map)
                           (dissoc result-map :exit :code :return-code :exit-code :map-step :commit-callback-ch :aggregator-notif-ch :error :data)))
        new-map-step (:map-step result-map)
        new-reduce-step (:reduce-step result-map)
        commit-callback-ch (:commit-callback-ch result-map)
        aggregator-notif-ch (:aggregator-notif-ch result-map)
        error? (:error result-map)
        exception (:error result-map)
        message-end (str "Step [" (:id step) "] finshed with result ["result"]\n")
        _ (log/debug message-end)
        next-step (find-next-step step result)
        _ (log/debug "Step " step-id ": Will terminate flow for this thread now and dispatch 4 join: " (and (next-step-join? job next-step) (dispatch4join? job)))
        next-step (if (and (next-step-join? job next-step) (dispatch4join? job)) nil next-step)
        _ (log/debug "Next step is " next-step)
        [retrying? step-retries] (if (and error? (not next-step))
                                   (assess4retry step step-retries)
                                   [false step-retries])
        next-step (if (and retrying? (not next-step)) step-id next-step)
        _ (if (and error? (not next-step)) (throw (:error result-map)))
        state (if next-step state :finished)
        finished? (= state :finished)
        step-end (java.util.Date.)
        history-map {:id step-id
                     :node-id node-id
                     :thread-stack thread-stack
                     :next-step next-step
                     :result result
                     :retry-count retry-count
                     :step-state (if error? :caught-error :completed)
                     :exception (when exception (Throwable->map exception))
                     :message message-end
                     :start step-start
                     :end step-end
                     :duration (- (.getTime step-end) (.getTime step-start))}
        job (assoc job :next-step next-step :state state
                       :step-state (if error? :caught-error :completed)
                       :aggregator-notif-ch aggregator-notif-ch
                       :properties (merge (:properties job) returned-props)
                       :map-steps (if new-map-step (assoc map-steps step-id new-map-step) map-steps)
                       :reduce-steps (if new-reduce-step (assoc reduce-steps step-id new-reduce-step) reduce-steps)
                       :step-retries step-retries
                       :history (conj (get job :history) history-map)
                       :end (when finished? step-end)
                       :duration (when finished? (- (.getTime step-end) (.getTime start))))]
    [commit-callback-ch job]))

(defn get-prop-trimming-fn [dont-log-properties trim-logged-properties properties-trim-size]
  (if dont-log-properties
    (fn [job]
      (assoc job :properties {}))
    (if (and trim-logged-properties properties-trim-size)
      (fn [job]
        (update job :properties (fn [p] (walk/postwalk #(cond
                                                          (vector? %) (if (> (count %) properties-trim-size) (subvec % 0 properties-trim-size) %)
                                                          (map? %) (if (> (count %) properties-trim-size) (into {} (take properties-trim-size %)) %)
                                                          :else %)
                                                       p))))
      identity)))

;;parallel threads: things to merge: history + properties
;;easy peasy: just add thread stack to every history record
;;things to merge in view:  current step + step status

;;TODO just use callback-ch property on job? -> NO NEED for :thread-stack outside of the main thread?!?
;;support for parallel steps:
#_{:parallel-threads [[:step-id {:main {:thread-id :main
                                        :next-step next-step
                                        :callback-chan callback-chan}
                                 :thread-1 {:thread-id :thread-1
                                            :next-step next-step
                                            :callback-chan callback-chan}}]]
   :thread-stack [[step-id thread-id][step-id thread-id][step-id thread-id]]}
;;TODO consider changing job id of non main threads (thread id and job id would be a new UUID)
(defn dispatch-job-threads! [out-jobs-ch {:keys [step next-step parallel-threads thread-call-stack] :as job}]
  "if multiple next steps exist all are dispatched in parallel. First one is always selected as :main thread that will carry jobs history and will orchestrate join of the threads when time comes.
  Each job thread is attached a :thread-stack and :parallel-threads vectors."
  (if-not (vector? next-step)
    (>!! out-jobs-ch job)
    (let [step-id (:id step)
          steps-maps (->> next-step
                          (map-indexed (fn [idx item] {:next-step item
                                                       :thread-id (if (zero? idx) :main (keyword (str "thread-" idx)))
                                                       :callback-chan (channel/mq-chan nil false)}))
                          vec)
          job (if (and (:parallel-threads job) (:thread-stack job))
                job
                (assoc job :parallel-threads [] :thread-stack []))]
      (log/info "Dispatching multiple parallel next steps: " steps-maps)
      (mapv (fn [{:keys [next-step thread-id callback-chan]}]
              (>!! out-jobs-ch (-> job
                                   (assoc :next-step next-step)
                                   (update-in [:parallel-threads] conj [step-id (util/keyify :thread-id steps-maps)])
                                   (update-in [:thread-stack] conj [step-id thread-id])
                                   (assoc :history (if (= :main thread-id) (:history job) [])))))
            steps-maps))))

(defn dispatch4join! [{:keys [thread-stack parallel-threads] :as job}]
  (let [thread (last thread-stack)
        thread-id (second thread)
        step-id (first thread)
        callback-chan (some-> parallel-threads
                          last
                          second
                          thread-id
                          :callback-chan)]
    (>!! callback-chan job)))

(defn trim-stack [job]
  (-> job
      (update :thread-stack pop)
      (update :parallel-threads pop)))

(defn new-history-stub [{:keys [step step-start] :as job}]
  (-> (select-keys job [:node-id :thread-stack :next-step :result :retry-count :step-state])
      (assoc :id (:id step) :start step-start)))

(defn add->history [job {:keys [step-end end message exception result] :as props}]
  (update job :history conj (-> (new-history-stub job)
                                (merge props (if (or step-end end) {:end (or step-end end)
                                                                    :duration (- (.getTime (or step-end end)) (.getTime (:step-start job)))} {})))))

(defn orchestrate-join! [{:keys [thread-stack parallel-threads step] :as job}]
  "To be performed from :main job thread. Orchestrates merge of other job threads (merges their properties into the current :main's).
  Returns job with merged properties and with thread stack and parallel-threads stack that do not contain data of the threads that were merged
  - i.e. are either empty or contain other outer threads that were not merged/dispatched yet."
  (let [merge-with-fn (if-let [f (-> (get-in step [:properties :merge-with-fn])
                                     exp/eval-property)]
                        #(merge-with (fn [i1 i2]
                                       (try (f i1 i2)
                                            (catch Exception e
                                              (log/warn "Failed to merge properties during join: " e)
                                              i2))) %1 %2)
                        merge)
        threads2merge (-> parallel-threads
                          last
                          second
                        (dissoc :main)
                        vals)
        async-ch-vec (mapv
                       #(thread (try
                                  (log/info "Waiting for thread [" (:thread-id %) "] to finish...")
                                  (let [j (<!! (:callback-chan %))]
                                    (log/info "Thread [" (:thread-id %) "] finished. Preparing for merge into the main thread...")
                                    [j (fn []
                                         (log/info "Acking message from thread [" (:thread-id %) "] and deleting its chan...")
                                         (channel/ack! (:callback-chan %) j)
                                         (channel/delete-mq-chan (:callback-chan %)))])
                                  (catch Exception e
                                    (log/warn e "Something went wrong during orchestration of a join of thread " (:thread-id %))
                                    [{:state :error :history [{:result :error :exception e}]} (fn [] (channel/delete-mq-chan (:callback-chan %)))])))
                       threads2merge)
        async-ch (async/merge async-ch-vec)]
    (loop [main-thread-job (trim-stack job)
           ack-fns-vec []]
      (let [[job-thread ack-fn] (async/<!! async-ch)];;TODO add timeout
        (if job-thread
          (do (log/debug "Merging job with thread stack [" (:thread-stack job-thread) "] into the main thread... " )
            (recur (-> main-thread-job
                       (update :properties merge-with-fn (:properties job-thread))
                       (update :history concat (:history job-thread))
                       (update :history #(into [] %))
                       (assoc :state (if (= :error (:state job-thread)) :error (:state main-thread-job)))
                       (assoc :step-state (if (or (= :error (:state job-thread)) (= :error (:state main-thread-job))) :error (:step-state main-thread-job)))
                       (add->history {:message (str "Merged thread with stack " (:thread-stack job-thread) " into main thread." )}))
                 (conj ack-fns-vec ack-fn)))
          (if-not (= :error  (:step-state main-thread-job))
            [main-thread-job ack-fns-vec]
            [(-> main-thread-job
                 (add->history
                           {:message (str "Join failed as there were errors in other job's threads.") :end (java.util.Date.) :exception (ex-info "Join failed as there were errors in other job's threads." {})})
                 (assoc :end (java.util.Date.)))
             ack-fns-vec]))))))

(defn orchestrate-step [{:keys [step thread-stack] :as job} node-id]
  "Wrapper function around process-step fn.
  Evaluates step's properties and processes the given step, but before that it also checks if step is of type join and if this is the main job thread - if so, then it also orchestrates the join.
  Returns a map containing job, a vector of ack functions that are to be called/committed later and a commit-callback-ch if acking is to be delayed (for map jobs)."
  (let [;; [commit-callback-ch job] (process-step job node-id) ;;FIXME swap order of these two lines?
        [main-thread-job ack-fns-vec] (if (= :join (:supertype step))
                                        (if (orchestrate-join? job)
                                          (do
                                            (log/debug "Orchestrating join thread-stack: " thread-stack " before processing step " (:id step))
                                            (orchestrate-join! job))
                                          (throw (IllegalStateException. "Join step should never be executed by a non-main thread!")))
                                        [job []])
        main-thread-job (exp/eval-ordered (:properties step) main-thread-job)
        _ (log/debug "Evaluated properties for job at step " (:id step) ": \n" (:properties main-thread-job))
        [commit-callback-ch main-thread-job] (if-not (= :error (:state main-thread-job))
                                               (process-step main-thread-job node-id)
                                               [nil main-thread-job])]
    {:job main-thread-job
     :ack-fns-vec ack-fns-vec
     :commit-callback-ch commit-callback-ch}))

(defn finalize-job! [{:keys [jobid thread-stack callback-ch] :as job} finished-ch ack-fns-vec update-cache-fn &[commit-callback-ch]]
  "Clears job's thread stack and finishes the job. If there are any pending threads then either dispatches this job thread for merge or (if it is the main) orchestrates the merge.
  Then it acks the job message and updates cache, job master thread is also sent to finished-ch for archival."
  (loop [job job
         ack-fns-vec ack-fns-vec
         thread-stack (or thread-stack [])]
    (log/debug "Initiating finalize-job! fn with thread-stack: " thread-stack " after step " (get-in job [:step :id]))
    (cond
      (dispatch4join? job) (do (log/debug "Dispatching 4 join with thread-stack: " thread-stack " after step " (get-in job [:step :id]))
                               (dispatch4join! job)
                               (mapv #(%) ack-fns-vec)
                               (update-cache-fn jobid job true))
      (orchestrate-join? job) (let [[{:keys [thread-stack] :as main-thread-job} new-ack-fns] (orchestrate-join! job)]
                                (recur main-thread-job (into [] (concat ack-fns-vec new-ack-fns)) thread-stack))
      (empty? thread-stack) (do
                              (>!! finished-ch job)
                              (if-not commit-callback-ch
                                (mapv (fn [f] (f))  ack-fns-vec)
                                (thread (do (<!! commit-callback-ch)
                                            (mapv (fn [f] (f))  ack-fns-vec))))
                              (log/info "Job " jobid " has finshed.")
                              (if callback-ch (>!! callback-ch job))
                              (update-cache-fn jobid job true)
                              :finished))))

;;TODO - will retries on error be done directly from here or will be handled by the "finished-ch" handler?
;;TODO add furhter channels based on job state - i.e. suspended and error?
;;TODO - out/finished/suspended channel could be also route to RDBMS - will be up to handler
(defn start-processor! [{:keys [stop-chan in-jobs-ch new-jobs-ch out-jobs-ch finished-ch state-agent eviction-agent mq-session node-id
                                cmd-exchange-ch server-config dont-log-properties trim-logged-properties properties-trim-size] :as config}]
  (thread ;;TODO use java Thread .start to allow getting Thread's handle and send interrupt signel during wait/IO etc.
    (let [prune-job (get-prop-trimming-fn dont-log-properties trim-logged-properties properties-trim-size)
          mark-for-eviction (fn [jobid] (send eviction-agent assoc jobid (java.util.Date.)))
          update-job-cache (fn [jobid job &[evict?]] (do (send state-agent assoc jobid (prune-job job))
                                                      (when evict? (mark-for-eviction jobid))))
          dont-evict (fn [jobid] (send eviction-agent dissoc jobid))]
      (binding [*cmd-exchange-ch* cmd-exchange-ch *server-config* server-config] ;;TODO this binding is needed only for map (splitter) type of step - not sure if it is elegant
        (channel/with-mq-session (:session mq-session)
                                 (loop []
                                   (channel/alt!!
                                     stop-chan :stopped
                                     [in-jobs-ch new-jobs-ch] ([m p] ;;FIXME update-job-cache + call dont-evict before calling initialize-step!
                                                                (let [{:keys [state step step-start jobdir jobid properties commands-ch thread-stack step-retries] :as job} (initialize-step (assoc m :step-start (java.util.Date.) :node-id node-id))
                                                                      retry-count (get step-retries (:id step))
                                                                      command nil ];;(poll! commands-ch)
                                                                  ;;TODO listen to a channel (will there be 1 channel for each job?) for lifecycle commands (pause/stop the job) - if suspended put to some "suspended" queue? Also jobs requiring human interaction will go there;
                                                                  (log/info "Retrieved job [" jobid "] from jobs channel; Starting step [" (:id step) "] with thread stack " (:thread-stack job) )
                                                                  (dont-evict jobid)
                                                                  (update-job-cache jobid job)
                                                                  (try
                                                                    (if command
                                                                      (case (.toLowerCase (str command));;TODO route to particular output channel + mark for eviction
                                                                        (:pause "pause" :suspend "suspend") (let [message "Retrieved pause command for this job, pausing..."
                                                                                                                  history-map {:message message :timestamp (java.util.Date.) :node-id node-id}
                                                                                                                  job (assoc m :state :suspended :history (conj (get m :history) history-map))]
                                                                                                              (log/info message)
                                                                                                              (update-job-cache jobid job)))
                                                                      (let [{commit-callback-ch :commit-callback-ch
                                                                             ack-fns :ack-fns-vec
                                                                             {:keys [next-step step state callback-ch aggregator-notif-ch thread-stack] :as job} :job} (orchestrate-step job node-id)
                                                                            ack-fns-vec (conj (or ack-fns []) #(do (log/info "Acking main message for step " (:id step) " with thread stack " thread-stack)
                                                                                                                   (channel/ack! p m)))]
                                                                        (case state ;;TODO add also :suspended state
                                                                          :running (do
                                                                                     (log/info "Next step is " next-step "; Submitting into jobs channel for next step's processing...")
                                                                                     (dispatch-job-threads! out-jobs-ch job)
                                                                                     (update-job-cache jobid job true)
                                                                                     (if-not commit-callback-ch
                                                                                       (mapv (fn [f] (f)) ack-fns-vec)
                                                                                       (thread (let [processed-id-tuples (<!! commit-callback-ch)]
                                                                                                 (>!! aggregator-notif-ch processed-id-tuples) ;;notify aggregator step - alternatively this is not needed as commit log is stored in the job folder
                                                                                                 (mapv (fn [f] (f)) ack-fns-vec))))
                                                                                     :running)
                                                                          :finished (finalize-job! job finished-ch ack-fns-vec update-job-cache)
                                                                          :error (finalize-job! job finished-ch ack-fns-vec update-job-cache))))
                                                                    (catch Exception e
                                                                      (log/warn e "Something went wrong during processing of a step. Stopping job...")
                                                                      (let [timestamp (java.util.Date.)
                                                                            history-map {:id (:id step) :step-state :error :thread-stack thread-stack :result :error :exception (Throwable->map e) :node-id node-id :retry-count retry-count :start step-start :end timestamp :duration (- (.getTime timestamp) (.getTime step-start))}
                                                                            job (assoc job :state :error :step-state :error :history (conj (get job :history) history-map) :end timestamp)]
                                                                        (finalize-job! job finished-ch [#(channel/ack! p m)] update-job-cache)))))
                                                                (recur))
                                     :priority true)))))));;TODO make priority configurable in config


(defrecord JobWorker [stop-chan in-jobs-ch new-jobs-ch out-jobs-ch finished-ch state-agent mq-session node-id cmd-exchange-ch
                      server-config dont-log-properties trim-logged-properties properties-trim-size]
  component/Lifecycle
  (start [this] ;;TODO use thread interrupt?
         (if-not stop-chan
           (let [stop-chan (chan (dropping-buffer 1))
                 this (assoc this :stop-chan stop-chan)]
            (log/info "Starting job worker....")
            (start-processor! this)
            this)))
  (stop [this]
        (log/info "Stopping job worker gracefully; sending a stop signal to the worker via service bus....")
        (>!! stop-chan :stop)
        (async/close! stop-chan)
        (Thread/sleep 100) ;;FIXME check whether it stopped - if not use thread interruption to stop wait/IO operations within worker!
        (assoc this :stop-chan nil)))

