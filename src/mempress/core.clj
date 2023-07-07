(ns mempress.core
  (:require [taoensso.nippy :as nippy]
            [taoensso.timbre :as timbre]
            [clj-time.coerce :as c]
            [clojure.core.memoize :as memo])
  (:import (clojure.lang IPersistentMap)
           (org.joda.time DateTime)
           #_(org.bson.types ObjectId)))

(def enabled (atom true))

(nippy/extend-freeze DateTime :clj-time/datetime
                     [x data-output]
                     (.writeLong data-output (c/to-long x)))

(nippy/extend-thaw :clj-time/datetime
                   [data-input]
                   (c/from-long (.readLong data-input)))

; an fyi - private fns from clojure.core which are explicitly accessed here:
;;parse-opts+specs
;;imap-cons
;;build-positional-factory
;;validate-fields

; also - private auxiliary fns from clojure.core which are implicitly used
;;maybe-destructured
;;parse-opts
;;parse-impls

; COMPACTABLE

(defprotocol Compactable
  (compact [c])
  (decompact [c])
  (fast-get-in [c ks] [c ks not-found])
  (compaction-ratio [c]))

(declare decomp)
(declare compactify)

;COOL UTILITIES

(declare compactable?)

(defn get-in
  ([m ks]
   (if (compactable? m)
     (fast-get-in m ks)
     (clojure.core/get-in m ks)))
  ([m ks not-found]
   (if (compactable? m)
     (fast-get-in m ks not-found)
     (clojure.core/get-in m ks not-found))))

(def s-dedup (atom nil))

(defn calc-size
  "Primitive 'real' memory footprint estimator, by walking the object graph.
   Supports nil, bool, bytes, numbers, strings, keywords, collections, org.joda.time.DateTime.
   Logs a warning per unsupported types otherwise."
  [o]
  (cond
    (nil? o) 0
    (boolean? o) 1
    (bytes? o) (count o)
    (number? o) 4
    (string? o) (cond
                  (nil? @s-dedup) (+ 8 20 (count o))
                  (contains? @s-dedup (hash o)) 8
                  :else (do (swap! s-dedup conj (hash o)) (+ 8 20 (count o))))
    (keyword? o) (+ 20 4)
    (char? o) 2
    (compactable? o) (max 20 (:compact-size (compaction-ratio o)))
    (map-entry? o) (+ 20 8 8 (calc-size (first o)) (calc-size (second o)))
    (map? o) (+ 20 (apply + (map calc-size o)))
    (coll? o) (+ 20 (apply + (map calc-size o)))
    (= (type o) DateTime) (+ 20 8)
    ;(= (type o) ObjectId) (+ 20 4 4 4 2)
    :else (do (timbre/warn "Unknown type to calc-size" {:type (type o)}) 0)))

(defn deep-merge
  "Merge nested maps recursively, otherwise all non-map items are atomic (pick last)
   Surgical/nested updates of maps vs overrides, is a special case that's treated with a special tag."
  [& maps]
  (letfn [(nesting-merge [& items]
            (let [lst (last items)]
              (if (every? map? items)
                (if (contains? lst :_atomic_value_mark)
                  (dissoc lst :_atomic_value_mark)
                  (apply merge-with nesting-merge items))
                lst)))]
    (apply merge-with nesting-merge maps)))

(defn keep-all
  "Returns a map containing only the provided paths."
  [m paths]
  (reduce (fn [m' path]
            (let [v (get-in m path :not-found)]
              (if (= :not-found v)
                m' (assoc-in m' path v)))) {} paths))

(defn dissoc-all
  "Returns a map removed of all the provided paths."
  [m paths]
  (reduce (fn [m' path]
            (if (> (count path) 1)
              (update-in m' (drop-last path) dissoc (last path))
              (dissoc m' (last path)))) m paths))

; the following is copy/pasted from (defrecord) with minor adjustments for the Compactable protocol

(defn- emit-defcrecord
  "Do not use this directly - use defrecord"
  {:added "1.2"}
  [tagname cname fields interfaces methods opts]
  (let [classname (with-meta (symbol (str (namespace-munge *ns*) "." cname)) (meta cname))
        interfaces (vec interfaces)
        interface-set (set (map resolve interfaces))
        methodname-set (set (map first methods))
        hinted-fields fields
        fields (vec (map #(with-meta % nil) fields))
        base-fields fields
        fields (conj fields '__meta '__extmap
                     '^:unsynchronized-mutable __hash
                     '^:unsynchronized-mutable __hasheq)
        type-hash (hash classname)]
    (when (some #{:volatile-mutable :unsynchronized-mutable} (mapcat (comp keys meta) hinted-fields))
      (throw (IllegalArgumentException. ":volatile-mutable or :unsynchronized-mutable not supported for record fields")))
    (let [gs (gensym)]
      (letfn
        [(irecord [[i m]]
           [(conj i 'clojure.lang.IRecord)
            m])
         (eqhash [[i m]]
           [(conj i 'clojure.lang.IHashEq)
            (conj m
                  `(hasheq [this#] (let [hq# ~'__hasheq]
                                     (if (zero? hq#)
                                       (let [h# (int (bit-xor ~type-hash (clojure.lang.APersistentMap/mapHasheq this#)))]
                                         (set! ~'__hasheq h#)
                                         h#)
                                       hq#)))
                  `(hashCode [this#] (let [hash# ~'__hash]
                                       (if (zero? hash#)
                                         (let [h# (clojure.lang.APersistentMap/mapHash this#)]
                                           (set! ~'__hash h#)
                                           h#)
                                         hash#)))
                  `(equals [this# ~gs] (clojure.lang.APersistentMap/mapEquals this# ~gs)))])
         (iobj [[i m]]
           [(conj i 'clojure.lang.IObj)
            (conj m `(meta [this#] ~'__meta)
                  `(withMeta [this# ~gs] (new ~tagname ~@(replace {'__meta gs} fields))))])
         (ilookup [[i m]]
           [(conj i 'clojure.lang.ILookup 'clojure.lang.IKeywordLookup)
            (conj m `(valAt [this# k#] (.valAt this# k# nil))
                  `(valAt [this# k# else#]
                          (case k# ~@(mapcat (fn [fld] [(keyword fld) fld])
                                             base-fields)
                                   (let [v# (get ~'__extmap k# else#)]
                                     (if (and (contains? (:cdat-ks ~'__extmap) k#)
                                              (or (identical? v# else#) (map? v#)))
                                       (.valAt (decomp this# ~'__extmap) k#)
                                       v#))))
                  `(getLookupThunk [this# k#]
                                   (let [~'gclass (class this#)]
                                     (case k#
                                       ~@(let [hinted-target (with-meta 'gtarget {:tag tagname})]
                                           (mapcat
                                             (fn [fld]
                                               [(keyword fld)
                                                `(reify clojure.lang.ILookupThunk
                                                   (get [~'thunk ~'gtarget]
                                                     (if (identical? (class ~'gtarget) ~'gclass)
                                                       (. ~hinted-target ~(symbol (str "-" fld)))
                                                       ~'thunk)))])
                                             base-fields))
                                       nil))))])
         (imap [[i m]]
           [(conj i 'clojure.lang.IPersistentMap)
            (conj m
                  `(count [this#] (+ ~(count base-fields)
                                     (count (dissoc ~'__extmap :cdat :cdat-ks :creal-size :chash))
                                     (count (:cdat-ks ~'__extmap))))
                  `(empty [this#] (throw (UnsupportedOperationException. (str "Can't create empty: " ~(str classname)))))
                  `(cons [this# e#] ((var clojure.core/imap-cons) this# e#))
                  `(equiv [this# ~gs]
                          (boolean
                            (or (identical? this# ~gs)
                                (when (identical? (class this#) (class ~gs))
                                  (let [~gs ~(with-meta gs {:tag tagname})
                                        this-keys# (set (keys ~'__extmap))
                                        that-keys# (set (keys (. ~gs ~'__extmap)))
                                        ]
                                    (cond
                                      ;original equiv implementation
                                      (and (not (this-keys# :cdat))
                                           (not (that-keys# :cdat)))
                                      (and ~@(map (fn [fld] `(= ~fld (. ~gs ~(symbol (str "-" fld))))) base-fields)
                                           (= ~'__extmap (. ~gs ~'__extmap)))
                                      ;fast keys-based negative comparison is available in this case
                                      (not= (into (:cdat-ks ~'__extmap #{})
                                                  (remove #{:cdat :cdat-ks :creal-size :chash} this-keys#))
                                            (into (:cdat-ks (. ~gs ~'__extmap) #{})
                                                  (remove #{:cdat :cdat-ks :creal-size :chash} that-keys#)))
                                      false
                                      ;fast hash-based comparison is available in this case
                                      (and (= this-keys# #{:cdat :cdat-ks :creal-size :chash})
                                           (= that-keys# #{:cdat :cdat-ks :creal-size :chash}))
                                      (= (:chash ~'__extmap) (:chash (. ~gs ~'__extmap)))
                                      ;fast(er) positive comparison
                                      (= (dissoc ~'__extmap :cdat) (dissoc (. ~gs ~'__extmap) :cdat))
                                      true
                                      ;decomp first, then equiv
                                      :else (.equiv (decomp this# ~'__extmap) (decomp ~gs (. ~gs ~'__extmap)))
                                      ))))))
                  `(containsKey [this# k#] (not (identical? this# (.valAt this# k# this#))))
                  `(entryAt [this# k#] (let [v# (.valAt this# k# this#)]
                                         (when-not (identical? this# v#)
                                           (clojure.lang.MapEntry/create k# v#))))
                  `(seq [this#] (if (:cdat ~'__extmap)
                                  (.seq (decomp this# ~'__extmap))
                                  (seq (concat [~@(map #(list `clojure.lang.MapEntry/create (keyword %) %) base-fields)]
                                               ~'__extmap))))
                  `(iterator [~gs]
                             (if (:cdat ~'__extmap)
                               (.iterator (decomp ~gs ~'__extmap))
                               (clojure.lang.RecordIterator. ~gs [~@(map keyword base-fields)] (clojure.lang.RT/iter ~'__extmap))))
                  `(assoc [this# k# ~gs]
                     (condp identical? k#
                       ~@(mapcat (fn [fld]
                                   [(keyword fld) (list* `new tagname (replace {fld gs} (remove '#{__hash __hasheq} fields)))])
                                 base-fields)
                       (new ~tagname
                            ~@(remove '#{__extmap __hash __hasheq} fields)
                            (assoc ~'__extmap k# (cond-> ~gs
                                                         (and (:cdat ~'__extmap)
                                                              (map? ~gs)
                                                              (contains? (:cdat-ks ~'__extmap) k#)
                                                              (not (contains? ~gs :_partial_compact_mark)))
                                                         (assoc :_atomic_value_mark true)
                                                         (map? ~gs)
                                                         (dissoc :_partial_compact_mark))))))
                  `(without [this# k#] (if (contains? (:cdat-ks ~'__extmap) k#)
                                         (.without (decomp this# ~'__extmap) k#)
                                         (if (contains? #{~@(map keyword base-fields)} k#)
                                           (dissoc (with-meta (into {} this#) ~'__meta) k#)
                                           (new ~tagname ~@(remove '#{__extmap __hash __hasheq} fields)
                                                (not-empty (dissoc ~'__extmap k#)))))))])
         (ijavamap [[i m]]
           [(conj i 'java.util.Map 'java.io.Serializable)
            (conj m
                  `(size [this#] (.count this#))
                  `(isEmpty [this#] (= 0 (.count this#)))
                  `(containsValue [this# v#] (boolean (some #{v#} (vals this#))))
                  `(get [this# k#] (.valAt this# k#))
                  `(put [this# k# v#] (throw (UnsupportedOperationException.)))
                  `(remove [this# k#] (throw (UnsupportedOperationException.)))
                  `(putAll [this# m#] (throw (UnsupportedOperationException.)))
                  `(clear [this#] (throw (UnsupportedOperationException.)))
                  `(keySet [this#] (set (keys this#)))
                  `(values [this#] (vals this#))
                  `(entrySet [this#] (set this#)))])
         (icompactable [[i m]]
           [(conj i (:on (deref (resolve 'Compactable))))
            (conj m
                  `(compact [this#] (compactify this# ~'__extmap))
                  `(decompact [this#] (decomp this# ~'__extmap))
                  `(fast-get-in [this# ks#] (fast-get-in this# ks# nil))
                  `(fast-get-in [this# ks# else#]
                                (if (= 1 (count ks#))
                                  (.valAt this# (first ks#) else#)
                                  (let [v# (clojure.core/get-in ~'__extmap ks# :not-found)]
                                    (if (= :not-found v#)
                                      (clojure.core/get-in this# ks# else#)
                                      v#))))
                  `(compaction-ratio [this#]
                                     (let [non-compact# (dissoc ~'__extmap :cdat :cdat-ks :creal-size :chash)
                                           non-compact-size# (calc-size non-compact#)
                                           non-compact-keys# (keys non-compact#)
                                           compact-keys# (seq (:cdat-ks ~'__extmap))
                                           all-keys# (not-empty (concat compact-keys# non-compact-keys#))
                                           raw-size# (+ non-compact-size# (:creal-size ~'__extmap 0))
                                           compact-size# (+ non-compact-size# (calc-size (not-empty (select-keys ~'__extmap [:cdat :cdat-ks :creal-size :chash]))))]
                                       {:raw-size         raw-size#
                                        :compact-size     compact-size#
                                        :all-keys         all-keys#
                                        :non-compact-keys non-compact-keys#
                                        :compact-keys     compact-keys#
                                        :compact-ratio    (if (zero? raw-size#) 1 (float (/ compact-size# raw-size#)))})))])
         ]
        (let [[i m] (-> [interfaces methods] irecord eqhash iobj ilookup imap ijavamap icompactable)]
          `(deftype* ~(symbol (name (ns-name *ns*)) (name tagname)) ~classname
                     ~(conj hinted-fields '__meta '__extmap
                            '^int ^:unsynchronized-mutable __hash
                            '^int ^:unsynchronized-mutable __hasheq)
                     :implements ~(vec i)
                     ~@(mapcat identity opts)
                     ~@m))))))

(defmacro defcrecord
  [name fields & opts+specs]
  (#'clojure.core/validate-fields fields name)
  (let [gname name
        [interfaces methods opts] (#'clojure.core/parse-opts+specs opts+specs)
        ns-part (namespace-munge *ns*)
        classname (symbol (str ns-part "." gname))
        hinted-fields fields
        fields (vec (map #(with-meta % nil) fields))]
    `(let []
       (declare ~(symbol (str '-> gname)))
       (declare ~(symbol (str 'map-> gname)))
       ~(emit-defcrecord name gname (vec hinted-fields) (vec interfaces) methods opts)
       (import ~classname)
       ~(#'clojure.core/build-positional-factory gname classname fields)
       (defn ~(symbol (str 'map-> gname))
         ~(str "Factory function for class " classname ", taking a map of keywords to field values.")
         ([m#] (~(symbol (str classname "/create"))
                 (if (instance? clojure.lang.MapEquivalence m#) m# (into {} m#)))))
       ~classname)))

; THE EMBODIMENT OF COMPACTABLE
(defcrecord CMap [])

(defn make-compactable
  "Compactable c'tor."
  ([] (->CMap))
  ([x] (map->CMap x)))

;;CACHE STUFF ---------------------------------------------

(def get-thaw-cache-key first)

(defn ^{:clojure.core.memoize/args-fn get-thaw-cache-key}
  nippy-thaw [cdat chash]
  (nippy/thaw cdat {:compressor nippy/lz4-compressor}))

(def memo-thaw (memo/lru #'nippy-thaw :lru/threshold 1024))

(defn evict-thaw-cache [& args]
  (memo/memo-clear! memo-thaw (get-thaw-cache-key args)))

(def cache? true)

;;END CACHE STUFF -----------------------------------------

(defn- decomp [cmap internal-model]
  (if-let [cdat (:cdat internal-model)]
    (cond-> cdat
            cache? (memo-thaw (:chash internal-model))
            (not cache?) (nippy/thaw {:compressor nippy/lz4-compressor})
            :also (deep-merge (dissoc internal-model :cdat :cdat-ks :creal-size :chash))
            :finally (make-compactable))
    cmap))

(defn- compactify [cmap internal-model]
  (if (= (keys internal-model) #{:cdat :cdat-ks :creal-size :chash})
    cmap
    (do
      (when cache?
        (evict-thaw-cache (:cdat internal-model) (:chash internal-model)))
      (let [cmap (decomp cmap internal-model)]
        (make-compactable
          {:cdat       (nippy/freeze cmap {:compressor nippy/lz4-compressor})
           :cdat-ks    (set (keys cmap))
           :creal-size (calc-size cmap)
           :chash      0})))))

(defn compactable? [x]
  (= (type x) (type (make-compactable))))

(defn compact-now [x]
  (let [cmap (if (compactable? x) x (make-compactable x))]
    (if @enabled
      (let [cmap' (compact cmap)] (if (and (:creal-size cmap')
                                           (< (/ (calc-size (select-keys cmap' [:cdat :cdat-ks :creal-size :chash]))
                                                 (:creal-size cmap'))
                                              (/ 1 3)))
                                    cmap' cmap))
      cmap)))

(defn decompact-now [x]
  (decompact (if (compactable? x) x (make-compactable x))))

(defn- partial-compact [to-comp to-keep-as-is default-val]
  (let [cmap (compact-now to-comp)]
    (if (:cdat cmap)
      (merge cmap to-keep-as-is)
      default-val)))

(defn- tag-partial-compaction-if-nesting [m [k & further-nested-path]]
  (if further-nested-path
    (-> (into {} m)
        (update k assoc :_partial_compact_mark true))
    m))

(defn- tag-partial-compaction [m ks]
  (reduce tag-partial-compaction-if-nesting m ks))

(defn compact-but-keep [x ks]
  (let [decompacted (decompact-now x)
        just-ks (-> decompacted
                    (keep-all ks)
                    (tag-partial-compaction ks))
        without-ks (dissoc-all decompacted ks)]
    (partial-compact without-ks just-ks decompacted)))

(defn compact-just [x ks]
  (let [decompacted (decompact-now x)
        just-ks (keep-all decompacted ks)
        without-ks (-> decompacted
                       (dissoc-all ks)
                       (tag-partial-compaction ks))]
    (partial-compact just-ks without-ks decompacted)))

(defn collify [o]
  (if (coll? o)
    o
    [o]))

(defn +sizes
  "Returns the provided object o, along with its compacted and raw sizes.
   If o is not compactable, its compacted and raw sizes are the same.
   The returned model is like so: [o [compact-size raw-size]]"
  [o]

  (if (compactable? o)
    [o ((juxt :compact-size :raw-size) (compaction-ratio o))]
    (let [bytes (try (calc-size o)
                     (catch Exception e
                       (timbre/warn e "failed to calc size for" (type o))
                       0))]
      [o [bytes bytes]])))

(defn compaction-summary [o]
  (swap! s-dedup #{})
  (let [coll (->> (collify o)
                  (pmap +sizes)
                  (map second))
        [cs rs] (reduce #(mapv + %1 %2) [0 0] coll)
        largest (when (not-empty coll) (apply max-key first coll))]
    (reset! s-dedup nil)
    {:compact cs :raw rs :ratio (float (/ cs rs)) :largest largest}))

(defn n-largest [n o]
  (swap! s-dedup #{})
  (let [top-n (->> (collify o)
                   (pmap +sizes)
                   (sort-by (fn [[_ [compact-size _]]] compact-size))
                   (reverse)
                   (take n))]
    (reset! s-dedup nil)
    top-n))

(defn n-largest-by [n f o]
  (->> (collify o)
       (group-by f)
       (map (juxt first (comp (partial n-largest n) second)))))
