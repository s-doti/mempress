(ns mempress.t-core
  (:require [midje.sweet :refer :all]
            [taoensso.timbre :as timbre]
            [mempress.core :as mempress]))

(background
  (around :facts
          (timbre/with-level :info
                             (let [initial-state @mempress/enabled]
                               (reset! mempress/enabled true)
                               ?form
                               (reset! mempress/enabled initial-state)))))

(midje.config/at-print-level
  :print-facts

  (fact "verify make-compactable via compaction-ratio"
        (mempress/compaction-ratio (mempress/make-compactable))
        => {:all-keys         nil
            :compact-keys     nil
            :compact-ratio    1
            :compact-size     0
            :non-compact-keys nil
            :raw-size         0}
        (mempress/compaction-ratio (mempress/make-compactable {:a 1}))
        => {:all-keys         '(:a)
            :compact-keys     nil
            :compact-ratio    1.0
            :compact-size     84
            :non-compact-keys '(:a)
            :raw-size         84})

  (fact "verify compact-now via compaction-ratio"
        (mempress/compaction-ratio (mempress/compact-now nil))
        => {:all-keys         nil
            :compact-keys     nil
            :compact-ratio    1
            :compact-size     0
            :non-compact-keys nil
            :raw-size         0}
        (mempress/compaction-ratio (assoc (mempress/compact-now {:a 1}) :b 1))
        => {:all-keys         '(:a :b)
            :compact-keys     nil
            :compact-ratio    1.0
            :compact-size     148
            :non-compact-keys '(:a :b)
            :raw-size         148}
        (mempress/compaction-ratio (mempress/decompact-now (mempress/compact-now {:a 1})))
        => {:all-keys         '(:a)
            :compact-keys     nil
            :compact-ratio    1.0
            :compact-size     84
            :non-compact-keys '(:a)
            :raw-size         84})

  (fact "verify compact-but-keep via compaction-ratio"
        (mempress/compaction-ratio (mempress/compact-but-keep {:a 1 :b 1} [[:b]]))
        => {:all-keys         '(:a :b)
            :compact-keys     nil
            :compact-ratio    1.0
            :compact-size     148
            :non-compact-keys '(:a :b)
            :raw-size         148}
        (mempress/compaction-ratio (mempress/compact-but-keep {:a 1 :b 1} [[:a] [:b]]))
        => {:all-keys         '(:a :b)
            :compact-keys     nil
            :compact-ratio    1.0
            :compact-size     148
            :non-compact-keys '(:a :b)
            :raw-size         148}
        (mempress/compaction-ratio (mempress/compact-but-keep {:a 1 :b 1} []))
        => {:all-keys         '(:a :b)
            :compact-keys     nil
            :compact-ratio    1.0
            :compact-size     148
            :non-compact-keys '(:a :b)
            :raw-size         148}
        (mempress/compaction-ratio (mempress/compact-but-keep
                                     {:a 1 :b 1 :c 1 :d 1 :e 1 :f {:g 1 :h {:i (repeat 1e3 1)}}}
                                     [[:a] [:b] [:c] [:f :g]]))
        => (mempress/compaction-ratio (reduce mempress/compact-but-keep
                                              {:a 1 :b 1 :c 1 :d 1 :e 1 :f {:g 1 :h {:i (repeat 1e3 1)}}}
                                              (repeat 5 [[:a] [:b] [:c] [:f :g]])))
        (let [partial-cmap (mempress/compact-but-keep
                             {:a 1 :b {:c 3 :d (repeat 1e3 4)}}
                             [[:b :c]])]
          ;verify compaction actually takes place as should
          (< (:compact-ratio (mempress/compaction-ratio partial-cmap)) 1)
          => truthy
          ;verify both non-compacted and compacted data nested on same path are accessible
          (keys (:b partial-cmap)) => (just [:c :d] :in-any-order)))

  (fact
    "compact-but-keep edge case is working as expected"
    ;before CYCO-14114 this test is failing for dropping newly added non-compactable data on the floor
    (let [well-compactable-data (repeat 1e3 1)
          non-compactable-data (repeatedly 1e3 rand)
          cmap (-> (mempress/compact-now {:a {:b well-compactable-data}})
                   (assoc-in [:a :d] non-compactable-data)
                   (mempress/compact-but-keep [[:a :b]]))]
      (keys (:a cmap)))
    => (just [:b :d] :in-any-order))

  (fact "verify compact-just via compaction-ratio"
        (mempress/compaction-ratio (mempress/compact-just {:a 1 :b 1} [[:a]]))
        => {:all-keys         '(:a :b)
            :compact-keys     nil
            :compact-ratio    1.0
            :compact-size     148
            :non-compact-keys '(:a :b)
            :raw-size         148}
        (mempress/compaction-ratio (mempress/compact-just {:a 1 :b 1} [[:a] [:b]]))
        => {:all-keys         '(:a :b)
            :compact-keys     nil
            :compact-ratio    1.0
            :compact-size     148
            :non-compact-keys '(:a :b)
            :raw-size         148}
        (mempress/compaction-ratio (mempress/compact-just {:a 1 :b 1} []))
        => {:all-keys         '(:a :b)
            :compact-keys     nil
            :compact-ratio    1.0
            :compact-size     148
            :non-compact-keys '(:a :b)
            :raw-size         148}
        (mempress/compaction-ratio (mempress/compact-just (mempress/compact-just
                                                            {:a 1 :b 1 :c 1 :d 1 :e 1 :f {:g 1 :h {:i (repeat 1e3 1)}}}
                                                            [[:f :h]]) [[:f :h]]))
        => (mempress/compaction-ratio (reduce mempress/compact-just
                                              {:a 1 :b 1 :c 1 :d 1 :e 1 :f {:g 1 :h {:i (repeat 1e3 1)}}}
                                              (repeat 5 [[:f :h]])))
        (let [partial-cmap (mempress/compact-just
                             {:a 1 :b {:c 3 :d (repeat 1e3 4)}}
                             [[:a] [:b :d]])]
          ;verify compaction actually takes place as should
          (< (:compact-ratio (mempress/compaction-ratio partial-cmap)) 1)
          => truthy
          ;verify both non-compacted and compacted data nested on same path are accessible
          (keys (:b partial-cmap)) => (just [:c :d] :in-any-order)))

  (fact "date-time works"
        (mempress/decompact-now (mempress/compact-now {:dt (clj-time.core/date-time 3000)}))
        => {:dt (clj-time.core/date-time 3000)})

  (fact "maintains equality"
        (let [cmap (mempress/compact-now {:a 1})]
          (= cmap (mempress/decompact-now cmap))) => truthy
        (let [cmap (mempress/compact-now {:a 1})]
          (= (mempress/decompact-now cmap) cmap)) => truthy
        (let [cmap (mempress/compact-now {:a (repeat 1e3 1)})]
          (= cmap (mempress/decompact-now cmap))) => truthy
        (let [cmap (mempress/compact-now {:a (repeat 1e3 1)})]
          (= (mempress/decompact-now cmap) cmap)) => truthy

        (let [cmap (mempress/compact-but-keep {:a {:b 1 :c 1}} [[:a :b]])]
          (= (mempress/decompact-now cmap) cmap)) => truthy
        (let [cmap (mempress/compact-but-keep {:a {:b 1 :c 1}} [[:a :b]])]
          (= cmap (mempress/decompact-now cmap))) => truthy
        (let [cmap (mempress/compact-but-keep {:a {:b 1 :c (repeat 1e3 1)}} [[:a :b]])]
          (= (mempress/decompact-now cmap) cmap)) => truthy

        (let [cmap (mempress/compact-but-keep {:a {:b 1 :c (repeat 1e3 1)}} [[:a :b]])]
          (= (update-in cmap [:a :c] (constantly (repeat 1e3 1))) cmap)) => truthy

        (let [cmap (mempress/compact-but-keep {:a {:b 1 :c (repeat 1e3 1)}} [[:a :b]])]
          (= cmap (mempress/decompact-now cmap))) => truthy

        (let [cmap (mempress/compact-just {:a {:b 1 :c 1}} [[:a :c]])]
          (= (mempress/decompact-now cmap) cmap)) => truthy
        (let [cmap (mempress/compact-just {:a {:b 1 :c 1}} [[:a :c]])]
          (= cmap (mempress/decompact-now cmap))) => truthy
        (let [cmap (mempress/compact-just {:a {:b 1 :c (repeat 1e3 1)}} [[:a :c]])]
          (= (mempress/decompact-now cmap) cmap)) => truthy
        (let [cmap (mempress/compact-just {:a {:b 1 :c (repeat 1e3 1)}} [[:a :c]])]
          (= cmap (mempress/decompact-now cmap))) => truthy
        )

  (fact "verify last update persists"
        (assoc (mempress/compact-now {:a 1}) :a 2) => {:a 2}
        (mempress/compact-now (assoc (mempress/compact-now {:a 1}) :a 2)) => {:a 2}
        (assoc (mempress/compact-now {:a {:b 1 :c 1}}) :a {:c 2}) => {:a {:c 2}})

  (fact "idempotent"
        (= (mempress/compact-now {:a (repeat 1e3 1)})
           (mempress/compact-now (mempress/compact-now {:a (repeat 1e3 1)}))) => truthy)

  (fact "order agnostic"
        (assoc (mempress/compact-now {:a 1}) :b 1) => (assoc (mempress/compact-now {:b 1}) :a 1))

  (fact "just cause"
        (:compact-ratio (mempress/compaction-ratio (mempress/compact-now {}))) => 1
        (:compact-ratio (mempress/compaction-ratio (mempress/compact-now {:a (repeat 1e3 1)}))) => (roughly 0.1 0.1))

  (fact "compact within single iteration"
        (mempress/compact-but-keep {:a {:b 1 :c 1}} [[:a :b]]) => {:a {:b 1 :c 1}}
        (provided
          (#'mempress/compactify anything anything) => (mempress/make-compactable) :times 1)
        (mempress/compact-just {:a {:b 1 :c 1}} [[:a :c]]) => {:a {:b 1 :c 1}}
        (provided
          (#'mempress/compactify anything anything) => (mempress/make-compactable) :times 1))

  (fact "equivalence"
        (= (mempress/compact-now {:a (repeat 1e3 1)}) (mempress/compact-now {:a (repeat 1e3 1)})) => truthy
        #_(provided
            (mempress/memo-thaw anything) => {:a (repeat 1e3 1)} :times 0))

  (fact "efficient dissoc"
        (dissoc (mempress/compact-now {:a (repeat 1e3 1)}) :a) => {}
        (provided
          (mempress/memo-thaw anything anything) => {:a 1} :times (if mempress/cache? 1 0))
        (dissoc (mempress/compact-but-keep {:a (repeat 1e3 1)} [[:a]]) :a) => {}
        (provided
          (mempress/memo-thaw anything anything) => {:a 1} :times 0))

  (fact "supports smart memoization"
        (if mempress/cache?
          (do (clojure.core.memoize/memo-reset! mempress/memo-thaw {})
              (let [cmap (mempress/compact-now {:a (repeat 1e3 (rand))})]
                ;1st decomp
                (mempress/decompact-now cmap) => {:mock 1}
                (provided
                  (taoensso.nippy/thaw anything anything) => {:mock 1} :times 1)
                ;2nd decomp
                (mempress/decompact-now cmap) => {:mock 1}
                (provided
                  (taoensso.nippy/thaw anything anything) => {:mock 2} :times 0)
                ;3rd mutate and decomp
                (-> cmap (assoc :what "ever") mempress/decompact-now) => {:mock 1 :what "ever"}
                (provided
                  (taoensso.nippy/thaw anything anything) => {:mock 3} :times 0)
                ;4th mutate, compact and decomp
                (-> cmap (assoc :what "ever") mempress/compact-now mempress/decompact-now) => {:mock 3 :what "ever"}
                (provided
                  (taoensso.nippy/thaw anything anything) => {:mock 3} :times 1))
              (clojure.core.memoize/memo-reset! mempress/memo-thaw {}))
          "it just works!" => "it just works!"))

  (fact "supports efficient retrieval"
        (let [cmap (mempress/compact-but-keep {:a {:b (repeat 1e3 1) :c 1}} [[:a :c]])]
          (with-redefs [mempress/decomp (fn [& args] (throw (Exception. "not efficient :(")))]
            ;; not efficient as get-in pulls :a first, then goes for c:
            (try (get-in cmap [:a :c]) (catch Exception e (.getMessage e))) => "not efficient :("
            ;; efficient cause using the get-in version of cutil
            (try (mempress/get-in cmap [:a :c]) (catch Exception e (.getMessage e))) => 1)))

  (fact "supports efficient mutations"
        (let [cmap (mempress/compact-but-keep {:a (repeat 1e3 1) :b (repeat 1e3 1)} [[:b]])]
          (with-redefs [mempress/decomp (fn [& args] (throw (Exception. "not efficient :(")))
                        mempress/compactify (fn [& args] (throw (Exception. "not efficient :(")))]
            (try (:a cmap) (catch Exception e (.getMessage e))) => "not efficient :("
            (try (:b cmap) (catch Exception e (.getMessage e))) => (repeat 1e3 1)
            ;not efficient 'cause update first gets
            (try (:a (update cmap :a (constantly 1))) (catch Exception e (.getMessage e))) => "not efficient :("
            (try (:b (update cmap :b (constantly 1))) (catch Exception e (.getMessage e))) => 1
            ;not efficient 'cause assoc-in first gets
            (try (:a (assoc-in cmap [:a :c] (constantly 1))) (catch Exception e (.getMessage e))) => "not efficient :("
            ;efficient 'cause assoc overrides, but only as long as no nesting
            (try (:a (assoc cmap :a 1)) (catch Exception e (.getMessage e))) => 1
            (try (:a (assoc cmap :a {})) (catch Exception e (.getMessage e))) => "not efficient :("))))

;without compaction

(midje.config/at-print-level
  :print-facts

  ;'component' / bdd tests
  (fact "actions without compaction"
        (assoc (mempress/make-compactable) :a 1) => {:a 1}
        (update (mempress/make-compactable) :a (constantly 1)) => {:a 1}
        (assoc-in (mempress/make-compactable) [:a :b] 1) => {:a {:b 1}}
        (update-in (mempress/make-compactable) [:a :b] (constantly 1)) => {:a {:b 1}}
        (count (mempress/make-compactable {})) => 0
        (empty? (mempress/make-compactable {})) => truthy
        (contains? (mempress/make-compactable {}) :a) => falsey
        (seq (mempress/make-compactable {:a 1})) => [[:a 1]]
        (dissoc (mempress/make-compactable {:a 1}) :a) => {}
        (keys (mempress/make-compactable {:a 1})) => [:a]
        (vals (mempress/make-compactable {:a 1})) => [1]
        (merge (mempress/make-compactable {:a 1}) {:b 1}) => {:a 1 :b 1}
        (into (mempress/make-compactable {:a 1}) {:b 1}) => {:a 1 :b 1}
        (conj (mempress/make-compactable {:a 1}) {:b 1}) => {:a 1 :b 1}
        )

  (fact "kw lookup without compaction"
        (:a (mempress/make-compactable {:a 1})) => 1
        (:b (mempress/make-compactable {:a 1})) => nil
        (:b (mempress/make-compactable {:a 1}) 1) => 1)

  (fact "get value without compaction"
        (get (mempress/make-compactable {:a 1}) :a) => 1
        (get (mempress/make-compactable {:a 1}) :b) => nil
        (get (mempress/make-compactable {:a 1}) :b 1) => 1)

  (fact "destructure without compaction"
        (let [{:keys [a] :as r} (mempress/make-compactable {:a 1})]
          [a r])
        => [1 (mempress/make-compactable {:a 1})])

  ;'unit' tests
  (fact "IHashEq without compaction"
        (.hasheq (mempress/make-compactable {:a 1})) => number?
        (.hashCode (mempress/make-compactable {:a 1})) => number?
        (.equals (mempress/make-compactable {:a 1}) {:a 1}) => true
        )

  (fact "IObj without compaction"
        (.meta (mempress/make-compactable {:a 1})) => nil
        (.withMeta (mempress/make-compactable {:a 1}) {}) => {:a 1}
        )

  (fact "ILookup and IKeywordLookup without compaction"
        (.valAt (mempress/make-compactable {:a 1}) :a) => 1
        (.valAt (mempress/make-compactable {:a 1}) :b 2) => 2
        (.getLookupThunk (mempress/make-compactable {:a 1}) :a) => nil
        )

  (fact "IPersistentMap without compaction"
        (.count (mempress/make-compactable {:a 1})) => 1
        (.empty (mempress/make-compactable {:a 1})) => (throws UnsupportedOperationException)
        (.cons (mempress/make-compactable {:a 1}) {:b 1}) => {:a 1 :b 1}
        (.equiv (mempress/make-compactable {:a 1}) (mempress/make-compactable {:a 1})) => true
        (.containsKey (mempress/make-compactable {:a 1}) :a) => true
        (.entryAt (mempress/make-compactable {:a 1}) :a) => [:a 1]
        (.seq (mempress/make-compactable {:a 1})) => [[:a 1]]
        (.next (.iterator (mempress/make-compactable {:a 1}))) => [:a 1]
        (.assoc (mempress/make-compactable {:a 1}) :b 1) => {:a 1 :b 1}
        (.without (mempress/make-compactable {:a 1}) :a) => {}
        )

  (fact "Map and Serializable without compaction"
        (.size (mempress/make-compactable {:a 1})) => 1
        (.isEmpty (mempress/make-compactable {:a 1})) => false
        (.containsValue (mempress/make-compactable {:a 1}) 1) => true
        (.get (mempress/make-compactable {:a 1}) :a) => 1
        (.put (mempress/make-compactable {:a 1}) :a 1) => (throws UnsupportedOperationException)
        (.remove (mempress/make-compactable {:a 1}) :a) => (throws UnsupportedOperationException)
        (.putAll (mempress/make-compactable {:a 1}) {:b 1}) => (throws UnsupportedOperationException)
        (.clear (mempress/make-compactable {:a 1})) => (throws UnsupportedOperationException)
        (.keySet (mempress/make-compactable {:a 1})) => #{:a}
        (.values (mempress/make-compactable {:a 1})) => [1]
        (.entrySet (mempress/make-compactable {:a 1})) => #{[:a 1]}))

;with compaction

(midje.config/at-print-level
  :print-facts

  ;'component' / bdd tests
  (fact "actions with compaction"
        (assoc (mempress/compact-now {}) :a 1) => {:a 1}
        (update (mempress/compact-now {}) :a (constantly 1)) => {:a 1}
        (assoc-in (mempress/compact-now {}) [:a :b] 1) => {:a {:b 1}}
        (update-in (mempress/compact-now {}) [:a :b] (constantly 1)) => {:a {:b 1}}
        (count (mempress/compact-now {})) => 0
        (empty? (mempress/compact-now {})) => truthy
        (contains? (mempress/compact-now {}) :a) => falsey
        (seq (mempress/compact-now {:a 1})) => [[:a 1]]
        (dissoc (mempress/compact-now {:a 1}) :a) => {}
        (keys (mempress/compact-now {:a 1})) => [:a]
        (vals (mempress/compact-now {:a 1})) => [1]
        (merge (mempress/compact-now {:a 1}) {:b 1}) => {:a 1 :b 1}
        (into (mempress/compact-now {:a 1}) {:b 1}) => {:a 1 :b 1}
        (conj (mempress/compact-now {:a 1}) {:b 1}) => {:a 1 :b 1}
        )

  (fact "kw lookup with compaction"
        (:a (mempress/compact-now {:a 1})) => 1
        (:b (mempress/compact-now {:a 1})) => nil
        (:b (mempress/compact-now {:a 1}) 1) => 1)

  (fact "get value with compaction"
        (get (mempress/compact-now {:a 1}) :a) => 1
        (get (mempress/compact-now {:a 1}) :b) => nil
        (get (mempress/compact-now {:a 1}) :b 1) => 1
        (get (mempress/compact-but-keep {:a {:b 1 :c (repeat 1e3 1)}} [[:a :b]]) :a)
        => {:b 1 :c (repeat 1e3 1)})

  (fact "destructure with compaction"
        (let [cmap (mempress/compact-now {:a {:b 1 :c (repeat 1e3 1)} :d 1})]
          (let [{{:keys [b]} :a :as r} cmap]
            [b (hash r)]) => [1 (hash cmap)])
        #_(let [cmap (mempress/compact-but-keep {:a 1 :t "d" :d {:r 1 :c (repeat 1e3 1)} #_#_:r 1} [[:a] [:r] [:t]])]
            (count (into {} (map (fn [{a :a :as cm}] [a cm]) [cmap]))) => 1
            (provided
              (#'mempress/decomp anything anything) => {:a 1 :d {:r 1 :c (repeat 1e3 1)} #_#_:r 1} :times 0)))

  (fact "somewhat more involved scenarios with nesting and reduction of nested data"
        ;support for these only introduced in context of CYCO-15980
        (let [easily-compactable {:a (repeat 1e3 1)}
              nested-data {1 {2 3 4 5} 6 7}
              cmap (-> easily-compactable
                       (assoc :d nested-data)
                       (mempress/compact-now))]
          (get (assoc cmap :d {1 2}) :d) => {1 2}
          (get (assoc cmap :e {1 2}) :e) => {1 2}           ;CYCO-17573
          (get (assoc cmap :d {1 {2 3}}) :d) => {1 {2 3}}
          (get (assoc cmap :d {8 9}) :d) => {8 9}
          (get (assoc-in cmap [:d] {1 2}) :d) => {1 2}
          (get (assoc-in cmap [:d] {1 {2 3}}) :d) => {1 {2 3}}
          (get (assoc-in cmap [:d] {8 9}) :d) => {8 9}
          (get (assoc-in cmap [:d 1] {8 9}) :d) => {1 {8 9} 6 7}
          (get (assoc-in cmap [:d 1] {2 3}) :d) => {1 {2 3} 6 7}
          (get (update cmap :d assoc-in [1] {4 5}) :d) => {1 {4 5}, 6 7}
          (get (update cmap :d assoc-in [1 4] 55) :d) => {1 {2 3 4 55}, 6 7}
          (get (update cmap :d dissoc 1) :d) => {6 7}
          (get (update-in cmap [:d] dissoc 1) :d) => {6 7}
          (get (update-in cmap [:d 1] dissoc 2) :d) => {1 {4 5} 6 7}
          ))

  ;'unit' tests
  (fact "IHashEq with compaction"
        (.hasheq (mempress/compact-now {:a 1})) => number?
        (.hashCode (mempress/compact-now {:a 1})) => number?
        (.equals (mempress/compact-now {:a 1}) {:a 1}) => true
        )

  (fact "IObj with compaction"
        (.meta (mempress/compact-now {:a 1})) => nil
        (.withMeta (mempress/compact-now {:a 1}) {}) => {:a 1}
        )

  (fact "ILookup and IKeywordLookup with compaction"
        (.valAt (mempress/compact-now {:a 1}) :a) => 1
        (.valAt (mempress/compact-now {:a 1}) :b 2) => 2
        (.getLookupThunk (mempress/compact-now {:a 1}) :a) => nil
        )

  (fact "IPersistentMap with compaction"
        (.count (mempress/compact-now {:a 1})) => 1
        (.empty (mempress/compact-now {:a 1})) => (throws UnsupportedOperationException)
        (.cons (mempress/compact-now {:a 1}) {:b 1}) => {:a 1 :b 1}
        (.equiv (mempress/compact-now {:a 1}) (mempress/make-compactable {:a 1})) => true
        (.equiv (mempress/make-compactable {:a 1}) (mempress/compact-now {:a 1})) => true
        (.containsKey (mempress/compact-now {:a 1}) :a) => true
        (.entryAt (mempress/compact-now {:a 1}) :a) => [:a 1]
        (.seq (mempress/compact-now {:a 1})) => [[:a 1]]
        (.next (.iterator (mempress/compact-now {:a 1}))) => [:a 1]
        (.assoc (mempress/compact-now {:a 1}) :b 1) => {:a 1 :b 1}
        (.without (mempress/compact-now {:a 1}) :a) => {}
        )

  (fact "Map and Serializable with compaction"
        (.size (mempress/compact-now {:a (repeat 1e3 1)})) => 1
        (.isEmpty (mempress/compact-now {:a 1})) => false
        (.containsKey (mempress/compact-now {:a 1}) :a) => true
        (.containsValue (mempress/compact-now {:a 1}) 1) => true
        (.get (mempress/compact-now {:a 1}) :a) => 1
        (.put (mempress/compact-now {:a 1}) :a 1) => (throws UnsupportedOperationException)
        (.remove (mempress/compact-now {:a 1}) :a) => (throws UnsupportedOperationException)
        (.putAll (mempress/compact-now {:a 1}) {:b 1}) => (throws UnsupportedOperationException)
        (.clear (mempress/compact-now {:a 1})) => (throws UnsupportedOperationException)
        (.keySet (mempress/compact-now {:a 1})) => #{:a}
        (.values (mempress/compact-now {:a 1})) => [1]
        (.entrySet (mempress/compact-now {:a 1})) => #{[:a 1]}
        )

  ;more unit tests

  (fact "deep-merge works"
        (mempress/deep-merge) => nil
        (mempress/deep-merge nil {1 1} nil {1 2} nil) => {1 2}
        (mempress/deep-merge {} {1 1} {} {1 2} {}) => {1 2}
        (mempress/deep-merge {1 2} {1 nil}) => {1 nil}

        (mempress/deep-merge {:a 1} {:b 1}) => {:a 1 :b 1}
        (mempress/deep-merge {:a 1} {:a 2}) => {:a 2}

        (mempress/deep-merge {:a {:b 1}} {:a {:b 2}}) => {:a {:b 2}}

        (mempress/deep-merge {:a {:b {:c 1}}} {:a {:b {:c 2}}}) => {:a {:b {:c 2}}}

        (mempress/deep-merge {:a {:b {:c 1}}} {:a {:b {:c' 2}}}) => {:a {:b {:c 1 :c' 2}}}
        ))

;performance
(comment (let [m {:a {:b 1 :c (repeat 1e3 1)} :d 1}
               m' (mempress/compact-now {:a {:b 1 :c (repeat 1e3 1)} :d 1})
               times 1e3
               bench-f #(do
                          (time (doseq [i (range times)] (:a %)))
                          (time (doseq [i (range times)] (:a % 1)))
                          (time (doseq [i (range times)] (get % :a)))
                          (time (doseq [i (range times)] (get % :a 1)))
                          (time (doseq [i (range times)] (get-in % [:a] 1)))
                          (time (doseq [i (range times)] (:d %)))
                          (time (doseq [i (range times)] (:d % 1)))
                          (time (doseq [i (range times)] (get % :d)))
                          (time (doseq [i (range times)] (get % :d 1)))
                          (time (doseq [i (range times)] (get-in % [:d] 1)))
                          (time (doseq [i (range times)] (get-in % [:a :b] 1)))
                          (time (doseq [i (range times)] (get-in % [:a :c] 1)))
                          (time (doseq [i (range times)] (assoc % :d 2)))
                          (time (doseq [i (range times)] (assoc % :a 2)))
                          (time (doseq [i (range times)] (update % :d inc)))
                          (time (doseq [i (range times)] (update % :a (constantly 2))))
                          (time (doseq [i (range times)] (assoc-in % [:a :b] 2)))
                          (time (doseq [i (range times)] (update-in % [:a :b] inc)))
                          (time (doseq [i (range times)] (dissoc % :a)))
                          (time (doseq [i (range times)] (dissoc % :d)))
                          (time (doseq [i (range times)] (count %)))
                          (time (doseq [i (range times)] (= % m')))
                          )]
           (do (prn "m") (bench-f m))
           (do (prn "compactable") (bench-f (mempress/make-compactable m)))
           (do (prn "compact-now") (bench-f (mempress/compact-now m)))
           (do (prn "compact-but-keep") (bench-f (mempress/compact-but-keep m [[:a :b] [:d]])))
           (do (prn "compact-just") (bench-f (mempress/compact-just m [[:a :c]])))
           ))