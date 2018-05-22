(ns titanoboa.util)

#?(:clj
    (defn store-file [dir filename bytes]
      (clojure.java.io/copy
        bytes
        (java.io.File. dir filename))))

(defn filter-by-index [coll idx]
  "takes collection and a collection of indexes - returns only elements on those indexes"
  (map (partial nth coll) idx))

(defn update-in-*
  [m [k & ks] f & args]
  (if (identical? k *)
    (let [idx (if (map? m) (keys m) (range (count m)))]
      (if ks
        (reduce #(assoc % %2 (apply update-in-* (get % %2) ks f args))
                m
                idx)
        (reduce #(assoc % %2 (apply f (get % %2) args))
                m
                idx)))
    (if ks
      (assoc m k (apply update-in-* (get m k) ks f args))
      (assoc m k (apply f (get m k) args)))))

(defn tokey [s]
  (if (and (= \: (first s)) (> (count s) 1))
    (keyword (subs s 1))
    s))

(defn s->key [s]
  (if (and (= \: (first s)) (> (count s) 1))
    (keyword (subs s 1))
    (keyword s)))

(defn keyify [key maps-array]
  "Takes array of maps (of presumably same structure and converts them into map of maps using the provided key"
  (reduce #(merge %1 {(key %2) %2}) {} maps-array))

(defn readable-interval [i]
  (let [ms (mod i 1000)
        s (/ (- i ms) 1000)
        secs (mod s 60)
        s (/ (- s secs) 60)
        mins (mod s 60)
        hrs (/ (- s mins) 60)]
    (cond
      (< i 1000) (str ms "ms")
      (< i (* 60 1000)) (str secs "s " ms "ms")
      (< i (* 60 60 1000)) (str mins "mins " secs "s " ms "ms")
      :else (str hrs "hrs " mins "mins " secs "s " ms "ms"))))

#?(:cljs
 (defn get-time-difference [start end]
  (let [end (or end (js/Date.))
        interval (- (.getTime end) (.getTime start))]
    (readable-interval interval)))
   :clj
  (defn get-time-difference [start end]
  (let [end (or end (java.util.Date.))
        interval (- (.getTime end) (.getTime start))]
    (readable-interval interval))))

(defn- shorten-uuid [uuid]
  (str (subs uuid 0 4) ".." (subs uuid (- (.-length uuid) 4) (.-length uuid))))
