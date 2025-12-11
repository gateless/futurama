(ns futurama.core-test
  (:require [bond.james :as bond]
            [clojure.core.async :refer [<! <!! >! go put! take! timeout] :as a]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [criterium.core :refer [quick-benchmark report-result
                                    with-progress-reporting]]
            [futurama.core :refer [!<! !<!! !<!* <!* async async-> async->>
                                   async-cancel! async-cancellable? async-completed?
                                   async-cancelled? async-every? async-for async-map
                                   async-postwalk async-prewalk async-reduce async-some
                                   async? get-pool thread with-pool] :as f])
  (:import [clojure.lang ExceptionInfo]
           [java.util.concurrent CompletableFuture Executors]))

(defn async-fixture
  [f]
  (f/set-async-factory! f/async-future-factory)
  (f/set-thread-factory! f/async-future-factory)
  (f)
  (f/set-async-factory! f/async-channel-factory)
  (f/set-thread-factory! f/async-channel-factory)
  (f)
  (f/set-async-factory! f/async-promise-factory)
  (f/set-thread-factory! f/async-promise-factory)
  (f)
  (f/set-async-factory! f/async-deferred-factory)
  (f/set-thread-factory! f/async-deferred-factory)
  (f)
  (f/set-async-factory! nil)
  (f/set-thread-factory! nil)
  (f))

(use-fixtures :once async-fixture)

(def ^:dynamic *test-val1* nil)
(def test-val2 nil)

(defmacro wrap-async
  [f & args]
  `(fn ~(symbol (str "async" (name f)))
     [& ~'argv]
     (async
       (apply ~f ~@args ~'argv))))

(defn plus-a-times-b
  [a b]
  (* (+ a 100) b))

(def test-pool
  (delay
    (Executors/newFixedThreadPool 10)))

(deftest cancel-async-test
  (testing "cancellable ->future is interrupted test"
    (with-pool @test-pool
      (let [interrupted (atom false)
            a (promise)
            s (atom 0)
            f (future
                (try
                  (while (not (async-cancelled?)) ;;; this loop goes on infinitely until the thread is interrupted
                    (Thread/sleep 90)
                    (println "thread looping..." (swap! s inc)))
                  (println "ended thread looping.")
                  (deliver a true)
                  (catch InterruptedException e
                    (println "interrupted looping by:" (type e))
                    (reset! interrupted true)
                    (deliver a true))))
            f' (f/->future f)]
        (is (true? (async-cancellable? f)))
        (go
          (<! (timeout 100))
          (async-cancel! f')) ;;; cancelling the ->future causes the converted async object to be interrupted
        (is (true? @a))
        (is (true? (async-cancelled? f)))
        (is (true? (async-completed? f)))
        (is (true? @interrupted)))))
  (testing "cancellable future is interrupted test"
    (with-pool @test-pool
      (let [interrupted (atom false)
            a (promise)
            s (atom 0)
            f (future
                (try
                  (while (not (async-cancelled?)) ;;; this loop goes on infinitely until the thread is interrupted
                    (Thread/sleep 90)
                    (println "thread looping..." (swap! s inc)))
                  (println "ended thread looping.")
                  (deliver a true)
                  (catch InterruptedException e
                    (println "interrupted looping by:" (type e))
                    (reset! interrupted true)
                    (deliver a true))))]
        (is (true? (async-cancellable? f)))
        (go
          (<! (timeout 100))
          (async-cancel! f)) ;;; cancelling the thread causes the backing thread to be interrupted
        (is (true? @a))
        (is (true? (async-cancelled? f)))
        (is (true? (async-completed? f)))
        (is (true? @interrupted)))))
  (testing "cancellable thread is interrupted test"
    (with-pool @test-pool
      (let [interrupted (atom false)
            a (promise)
            s (atom 0)
            f (f/thread
                (try
                  (while (not (async-cancelled?)) ;;; this loop goes on infinitely until the thread is interrupted
                    (Thread/sleep 90)
                    (println "thread looping..." (swap! s inc)))
                  (println "ended thread looping.")
                  (deliver a true)
                  (catch InterruptedException e
                    (println "interrupted looping by:" (type e))
                    (reset! interrupted true)
                    (deliver a true))))]
        (is (true? (async-cancellable? f)))
        (go
          (<! (timeout 100))
          (async-cancel! f)) ;;; cancelling the thread causes the backing thread to be interrupted
        (is (true? @a))
        (is (true? (async-cancelled? f)))
        (is (true? (async-completed? f)))
        (if (= f/*thread-factory* f/async-channel-factory) ;;; only true when using future based threads)
          (is (false? @interrupted))
          (is (true? @interrupted))))))
  (testing "cancellable async block is interrupted test"
    (with-pool @test-pool
      (let [interrupted (atom false)
            a (promise)
            s (atom 0)
            f (f/async
                (try
                  (while (not (async-cancelled?)) ;;; this loop goes on infinitely until the thread is interrupted
                    (Thread/sleep 90)
                    (println "thread looping..." (swap! s inc)))
                  (println "ended thread looping.")
                  (deliver a true)
                  (catch InterruptedException e
                    (println "interrupted looping by:" (type e))
                    (reset! interrupted true)
                    (deliver a true))))]
        (is (true? (async-cancellable? f)))
        (go
          (<! (timeout 100))
          (async-cancel! f)) ;;; cancelling the thread causes the backing thread to be interrupted
        (is (true? @a))
        (is (true? (async-cancelled? f)))
        (is (true? (async-completed? f)))
        (if (= f/*async-factory* f/async-channel-factory) ;;; only true when using future based threads)
          (is (false? @interrupted))
          (is (true? @interrupted))))))
  (testing "cancellable nested async cancellable is cancelled test"
    (with-pool @test-pool
      (let [interrupted (atom false)
            a (promise)
            s (atom 0)
            f (async
                (CompletableFuture/completedFuture
                 (thread
                   (async
                     (try
                       (while (not (async-cancelled?)) ;;; this loop goes on infinitely until the thread is interrupted
                         (Thread/sleep 90)
                         (println "thread looping..." (swap! s inc)))
                       (println "ended thread looping.")
                       (deliver a true)
                       (catch InterruptedException e
                         (println "interrupted looping by:" (type e))
                         (reset! interrupted true)
                         (deliver a true)))))))]
        (is (true? (async-cancellable? f)))
        (go
          (<! (timeout 100))
          (async-cancel! f)) ;;; cancelling the thread causes the backing thread to be interrupted
        (is (true? @a))
        (is (true? (async-cancelled? f)))
        (is (true? (async-completed? f)))
        (is (false? @interrupted))))))

(deftest with-pool-macro-test
  (testing "with-pool evals body with provided pool"
    (bond/with-spy [get-pool]
      (!<!!
       (with-pool @test-pool
         (async
           (is (= 100
                  (!<! (CompletableFuture/completedFuture 100)))))))
      (is (= [] (->> get-pool bond/calls (map :args))))))
  (testing "with-pool uses specified workload pool - io"
    (let [io-pool (get-pool :io)]
      (bond/with-spy [get-pool]
        (!<!!
         (with-pool :io
           (async
             (is (= 100
                    (!<! (CompletableFuture/completedFuture 100))))
             (is (= io-pool f/*thread-pool*)))))
        (is (= [[:io]] (->> get-pool bond/calls (map :args)))))))
  (testing "with-pool uses specified workload pool - mixed"
    (let [mixed-pool (get-pool :mixed)]
      (bond/with-spy [get-pool]
        (!<!!
         (with-pool :mixed
           (async
             (is (= 100
                    (!<! (CompletableFuture/completedFuture 100))))
             (is (= mixed-pool f/*thread-pool*)))))
        (is (= [[:mixed]] (->> get-pool bond/calls (map :args)))))))
  (testing "with-pool uses specified workload pool - compute"
    (let [compute-pool (get-pool :compute)]
      (bond/with-spy [get-pool]
        (!<!!
         (with-pool :compute
           (async
             (is (= 100
                    (!<! (CompletableFuture/completedFuture 100))))
             (is (= compute-pool f/*thread-pool*)))))
        (is (= [[:compute]] (->> get-pool bond/calls (map :args))))))))

(deftest thread-macro-workload-test
  (testing "thread uses workload pool - io"
    (bond/with-spy [get-pool]
      (is (= ::done
             (!<!!
              (thread :io
                ::done))))
      (is (= [[:io]] (->> get-pool bond/calls (map :args))))))
  (testing "thread uses default pool - mixed"
    (bond/with-spy [get-pool]
      (is (= ::done
             (!<!!
              (thread
                ::done))))
      (is (= [[:mixed]] (->> get-pool bond/calls (map :args))))))
  (testing "thread uses workload pool - compute"
    (bond/with-spy [get-pool]
      (is (= ::done
             (!<!!
              (thread :compute
                ::done))))
      (is (= [[:compute]] (->> get-pool bond/calls (map :args)))))))

(deftest async-macro-workload-test
  (testing "thread uses workload pool - io"
    (bond/with-spy [get-pool]
      (is (= ::done
             (!<!!
              (async :io
                ::done))))
      (is (= [[:io]] (->> get-pool bond/calls (map :args))))))
  (testing "thread uses default pool - io"
    (bond/with-spy [get-pool]
      (is (= ::done
             (!<!!
              (async
                ::done))))
      (is (= [[:io]] (->> get-pool bond/calls (map :args))))))
  (testing "thread uses workload pool - compute"
    (bond/with-spy [get-pool]
      (is (= ::done
             (!<!!
              (async :compute
                ::done))))
      (is (= [[:compute]] (->> get-pool bond/calls (map :args)))))))

(deftest thread-first-macro-tests
  (testing "can thread first async->"
    (is (= 1500
           (-> 10
               (+ 10)
               (* 5)
               (+ 100)
               (plus-a-times-b 5))
           (<!!
            (async-> 10
                     ((wrap-async +) 10)
                     ((wrap-async *) 5)
                     ((wrap-async +) 100)
                     ((wrap-async plus-a-times-b) 5)))))))

(deftest thread-last-macro-tests
  (testing "can thread first async->>"
    (is (= 21000
           (->> 10
                (+ 10)
                (* 5)
                (+ 100)
                (plus-a-times-b 5))
           (<!!
            (async->> 10
                      ((wrap-async +) 10)
                      ((wrap-async *) 5)
                      ((wrap-async +) 100)
                      ((wrap-async plus-a-times-b) 5)))))))

(deftest async-some-test
  (testing "async-some async test returns first returned valid logical true"
    (is (= 9 ;;; always returns 9 even though it's the last number because it is returned first
           (!<!! (async-some
                  (fn [n]
                    (async
                      (when (odd? n)
                        (!<! (timeout (- 1000 (* n 100))))
                        n)))
                  (range 10)))))))

(deftest async-every-test
  (testing "async-every? async test returns true when all true"
    (is (= true
           (!<!! (async-every?
                  (fn [n]
                    (async
                      (when (number? n)
                        (!<! (timeout 50))
                        true)))
                  (range 10))))))
  (testing "async-every? async test returns false when some false"
    (is (= false
           (!<!! (async-every?
                  (fn [n]
                    (async
                      (when (not= n 5)
                        (!<! (timeout 50))
                        true)))
                  (range 10)))))))

(deftest async-reduce-test
  (testing "async reduce async result handling"
    (is (= 55
           (!<!! (async-reduce (fn [& nsq]
                                 (if (empty? nsq)
                                   10
                                   (async
                                     (apply + nsq))))
                               (async (range 10)))))))
  (testing "async reduce async result handling - provide init"
    (is (= 55
           (!<!! (async-reduce (fn [total number]
                                 (async
                                   (+ total number))) 10 (async (range 10)))))))
  (testing "async reduce async result handling - reduce a map"
    (is (= {:foo 1
            :bar 2
            :zlu 10}
           (!<!! (async-reduce (fn [m k v]
                                 (async
                                   (assoc m k (inc v))))
                               {} (async {:foo 0
                                          :bar 1
                                          :zlu 9})))))))

(deftest async-prewalk-test
  (testing "async prewalk async walk handler"
    (is (= {:foo [:bar 100 #{1 2 3 4 5}]}
           (!<!! (async-prewalk (fn [n]
                                  (async n))
                                (CompletableFuture/completedFuture
                                 {:foo
                                  (CompletableFuture/completedFuture
                                   [:bar (go 100) (async #{1 2 3 4 5})])})))))))

(deftest async-postwalk-test
  (testing "async postwalk async walk handler"
    (is (= {:foo [:bar 100 #{1 2 3 4 5}]}
           (!<!! (async-postwalk (fn [n]
                                   (async n))
                                 (CompletableFuture/completedFuture
                                  {:foo
                                   (CompletableFuture/completedFuture
                                    [:bar (go 100) (async #{1 2 3 4 5})])})))))))

(deftest async-map-test
  (testing "works the same way as a map fn with multiple colls"
    (let [async-handler #(async (apply + %&))
          args (repeat 10 (range 10))]
      (is (= [0 10 20 30 40 50 60 70 80 90]
             (apply map + args)
             (!<!! (apply async-map async-handler args))))))
  (testing "can loop map concurrently, performance test"
    (let [_pool-warmup (<!! (async-map #(async (!<! (timeout 50)) (inc %)) (range 20)))
          bench (with-progress-reporting
                  (quick-benchmark
                   (<!! (async-map #(async (!<! (timeout 50)) (inc %)) (range 10)))
                   {:verbose true}))
          [mean [lower upper]] (:mean bench)]
      (report-result bench)
      (is (<= 0.04 lower mean upper 0.07)))))

(deftest async-for-test
  (testing "can loop for concurrently, performance test"
    (let [bench (with-progress-reporting
                  (quick-benchmark
                   (<!!
                    (async-for
                     [a (range 4)
                      b (range 4)
                      :let [c (+ a b)]
                      :when (and (odd? a) (odd? b))]
                     (async
                       (!<! (timeout 50))
                       [a b c (+ a b c)])))
                   {:verbose true}))
          [mean [lower upper]] (:mean bench)]
      (report-result bench)
      (is (<= 0.04 lower mean upper 0.07)))))

(deftest async-ops
  (testing "async? for CompletableFuture"
    (is (true? (async? (CompletableFuture/completedFuture "yes")))))
  (testing "async? for core.async channel"
    (is (true? (async? (go "yes")))))
  (testing "async? for raw value"
    (is (false? (async? "no"))))
  (testing "raw value handling - !<!"
    (let [v {:foo "bar"}]
      (is (= v (!<!! (async (!<! v)))))))
  (testing "raw value handling - !<!!"
    (let [v {:foo "bar"}]
      (is (= v (!<!! v)))))
  (testing "async put! test"
    (let [^CompletableFuture f (CompletableFuture.)
          v {:foo "bar"}]
      (put! f v)
      (put! f {:foo "baz"})
      (is (= v @f))))
  (testing "async take! test"
    (let [^CompletableFuture f (CompletableFuture.)
          v {:foo "bar"}
          p (promise)]
      (take! f (partial deliver p))
      (.complete f v)
      (is (= v @p))))
  (testing "async take! nested test"
    (let [^CompletableFuture f (CompletableFuture/completedFuture
                                (CompletableFuture/completedFuture {:foo "bar"}))
          v {:foo "bar"}
          r (<!! f)]
      (is (= v r))))
  (testing "bindings test blocking - !<!!"
    (binding [*test-val1* 100]
      (with-redefs [test-val2 200]
        (is (= [100 200]
               (!<!!
                (async
                  [*test-val1* test-val2]))
               (!<!!
                (thread
                  [*test-val1* test-val2])))))))
  (testing "bindings test non-blocking - !<!"
    (binding [*test-val1* 100]
      (with-redefs [test-val2 200]
        (<!!
         (go
           (is (= [100 200]
                  (!<!
                   (async
                     [*test-val1* test-val2]))
                  (!<!
                   (thread
                     [*test-val1* test-val2])))))))))
  (testing "sequential collection non-blocking take - <!*"
    (<!!
     (async
       (is (= (range 1 11)
              (<!*
               (for [n (range 10)]
                 (async (inc n)))))))))
  (testing "sequential collection non-blocking take - !<!*"
    (<!!
     (async
       (is (= (range 1 11)
              (!<!*
               (for [n (range 10)]
                 (async (inc n)))))))))
  (testing "nested blocking take - !<!!"
    (is (= {:foo "bar"}
           (!<!! (async
                   (go
                     (CompletableFuture/completedFuture
                      (thread
                        (go
                          (<! (timeout 50))
                          (let [c (CompletableFuture.)]
                            (>! c {:foo "bar"})
                            (delay
                              (future
                                (atom
                                 (let [p (promise)]
                                   (deliver p c)
                                   p))))))))))))))
  (testing "nested non-blocking take - !<!"
    (<!!
     (async
       (is (= {:foo "bar"}
              (!<! (async
                     (go
                       (CompletableFuture/completedFuture
                        (thread
                          (go
                            (<! (timeout 50))
                            (delay
                              (future
                                (let [p (promise)]
                                  (deliver p
                                           (CompletableFuture/completedFuture {:foo "bar"}))
                                  p)))))))))))))))

(deftest error-handling
  (testing "throws async exception on blocking take from thread - !<!!"
    (is (thrown-with-msg?
         ExceptionInfo #"foobar"
         (!<!! (thread
                 (throw (ex-info "foobar" {}))
                 ::result)))))
  (testing "throws async exception on non-blocking take from thread - !<!"
    (<!!
     (async
       (is (thrown-with-msg?
            ExceptionInfo #"foobar"
            (!<! (thread
                   (throw (ex-info "foobar" {}))
                   ::result)))))))
  (testing "throws async exception on blocking take from async - !<!!"
    (is (thrown-with-msg?
         ExceptionInfo #"foobar"
         (!<!! (async
                 (throw (ex-info "foobar" {}))
                 ::result)))))
  (testing "throws async exception on non-blocking take from async - !<!"
    (<!!
     (async
       (is (thrown-with-msg?
            ExceptionInfo #"foobar"
            (!<! (async
                   (throw (ex-info "foobar" {}))
                   ::result))))))))

(deftest test-future-conversion
  (testing "can convert any async result to CompletableFuture - success"
    (let [fut (f/->future (async ::foobar))]
      (is (= ::foobar @fut))))
  (testing "can convert any async result to CompletableFuture - failure"
    (let [fut (f/->future (async (throw (ex-info "foobar" {}))))]
      (is (thrown-with-msg? Exception #"foobar" @fut))))
  (testing "can convert any thread result to CompletableFuture"
    (let [fut (f/->future (thread ::foobar))]
      (is (= ::foobar @fut))))
  (testing "can use CompletableFuture as CompletableFuture"
    (let [fut' (CompletableFuture/completedFuture ::foobar)
          fut (f/->future fut')]
      (is (= ::foobar @fut))
      (is (identical? fut' fut))))
  (testing "can use non-async value as CompletableFuture"
    (let [val ::foobar
          fut (f/->future val)]
      (is (= ::foobar @fut)))))
