(ns futurama.core-test
  (:require [bond.james :as bond]
            [clojure.core.async :refer [<! <!! >! go put! take! timeout] :as a]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [futurama.core :refer [!<! !<!! !<!* <!* async async-> async->>
                                   async-cancel! async-cancellable? async-completed?
                                   async-cancelled? async-every? async-for async-map
                                   async-postwalk async-prewalk async-reduce async-some
                                   async? get-pool thread with-pool] :as f]
            [futurama.impl :as impl])
  (:import [clojure.lang ExceptionInfo]
           [java.util.concurrent CompletableFuture Executor ExecutorService Executors TimeUnit]))

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

(def ^:dynamic *bind-probe* nil)

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

(defn min-elapsed-ms
  "Runs `thunk` `n` times and returns the minimum wall-clock elapsed time in ms.
   Taking the minimum filters out GC/scheduling noise so we can assert tight,
   meaningful concurrency bounds cheaply — without criterium's statistical runs."
  [n thunk]
  (reduce
   min
   (for [_ (range n)]
     (let [start (System/nanoTime)]
       (thunk)
       (/ (- (System/nanoTime) start) 1e6)))))

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
            f (thread
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
            f (async
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
    (let [run     #(<!! (async-map (fn [x] (async (!<! (timeout 50)) (inc x)))
                                   (range 10)))
          _warmup (run)
          elapsed (min-elapsed-ms 10 run)]
      (is (<= 40 elapsed 150)
          (str "expected concurrent execution ~50ms, got " elapsed "ms")))))

(deftest async-for-test
  (testing "works the same way as a for comprehension with multiple colls"
    (let [args1 (range 10)
          args2 (range 10)]
      (is (= (for [x args1
                   y args2]
               (+ x y))
             (<!! (async-for [x args1
                              y args2]
                             (async (+ x y))))))))
  (testing "can loop for concurrently, performance test"
    (let [run     #(<!! (async-for
                         [a (range 4)
                          b (range 4)
                          :let [c (+ a b)]
                          :when (and (odd? a) (odd? b))]
                         (async
                           (!<! (timeout 50))
                           [a b c (+ a b c)])))
          _warmup (run)
          elapsed (min-elapsed-ms 10 run)]
      (is (<= 40 elapsed 150)
          (str "expected concurrent execution ~50ms, got " elapsed "ms")))))

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

(defn- probe-binding
  "Run a binding probe n times, returning a frequency map of the results. The probe is a function that
  returns the value of the dynamic var *bind-probe* after an async operation. If the binding is lost,
  the probe will throw an exception, which is caught and recorded as :threw."
  [n take-cb]
  (frequencies
   (repeatedly n
               (fn []
                 (try (take-cb) (catch Throwable _ :threw))))))

(deftest binding-bound-outside-async-and-go-block-survives-park
  (testing "binding set outside async is never lost across an !<! park with 0 loss"
    (let [n 2000
          f (fn []
              (async :ignore))
          freqs (probe-binding n (fn []
                                   (binding [*bind-probe* :bound]
                                     (!<!! (async
                                             (!<! (f))
                                             *bind-probe*)))))]
      (is (= {:bound n} freqs)
          (str "binding lost with bind-outside: " freqs))))
  (testing "binding set outside go is never lost across an <! park with 0 loss"
    (let [n 2000
          f (fn []
              (go :ignore))
          freqs (probe-binding n (fn []
                                   (binding [*bind-probe* :bound]
                                     (<!! (go
                                            (<! (f))
                                            *bind-probe*)))))]
      (is (= {:bound n} freqs)
          (str "binding lost with bind-outside: " freqs)))))

(deftest binding-bound-inside-async-and-go-block-survives-park
  (testing "binding set inside async survives an !<! park (window widened for determinism) with 0 loss"
    (let [ioc-take @#'impl/ioc-take!
          n 500
          f (fn []
              (async :ignore))]
      (with-redefs [impl/ioc-take! (fn [state blk c]
                                     (let [r (ioc-take state blk c)]
                                       (when (nil? r)
                                         (java.util.concurrent.locks.LockSupport/parkNanos 200000))
                                       r))]
        (let [freqs (probe-binding n (fn []
                                       (!<!! (async
                                               (binding [*bind-probe* :bound]
                                                 (!<! (f))
                                                 *bind-probe*)))))]
          (is (= {:bound n} freqs)
              (str "binding lost across park: " freqs))))))
  (testing "binding set inside go block survives an <! park (window widened for determinism) with 0 loss"
    (let [ioc-take @#'impl/ioc-take!
          n 500
          f (fn []
              (go :ignore))]
      (with-redefs [impl/ioc-take! (fn [state blk c]
                                     (let [r (ioc-take state blk c)]
                                       (when (nil? r)
                                         (java.util.concurrent.locks.LockSupport/parkNanos 200000))
                                       r))]
        (let [freqs (probe-binding n (fn []
                                       (<!! (go
                                              (binding [*bind-probe* :bound]
                                                (<! (f))
                                                *bind-probe*)))))]
          (is (= {:bound n} freqs)
              (str "binding lost across park: " freqs)))))))

(deftest non-async-fast-path
  ;; !<! / !<!! short-circuit non-async values, returning them directly without
  ;; a channel round-trip. These guard that behavior, including that the
  ;; argument expression is evaluated exactly once (macro hygiene).
  (testing "!<!! returns nil for a nil value"
    (is (nil? (!<!! nil))))
  (testing "!<!! returns a raw scalar unchanged"
    (is (= 42 (!<!! 42))))
  (testing "!<! returns nil for a nil value"
    (is (nil? (<!! (async (!<! nil))))))
  (testing "!<! returns a raw scalar unchanged"
    (is (= 42 (<!! (async (!<! 42))))))
  (testing "!<!! evaluates a non-async argument expression exactly once"
    (let [calls (atom 0)]
      (is (= 1 (!<!! (swap! calls inc))))
      (is (= 1 @calls))))
  (testing "!<! evaluates a non-async argument expression exactly once"
    (let [calls (atom 0)]
      (is (= 1 (<!! (async (!<! (swap! calls inc))))))
      (is (= 1 @calls))))
  (testing "!<!! evaluates an async argument expression exactly once"
    (let [calls (atom 0)]
      (is (= 1 (!<!! (async (swap! calls inc)))))
      (is (= 1 @calls))))
  (testing "!<! evaluates an async argument expression exactly once"
    (let [calls (atom 0)]
      (is (= 1 (<!! (async (!<! (async (swap! calls inc)))))))
      (is (= 1 @calls)))))

(deftest async-reader-read-port-take
  (testing "reads a plain (non-async) wrapped value directly"
    (is (= 42 (<!! (f/->async-reader 42))))
    (is (nil? (<!! (f/->async-reader nil)))))
  (testing "poll! fast-path: reads a ready value from a wrapped channel"
    (let [ch (a/chan 1)]
      (a/>!! ch :ready)
      (is (= :ready (<!! (f/->async-reader ch))))))
  (testing "completable-reader fast-path spot check across read-port types"
    (is (= :fut (!<!! (future :fut))))
    (is (= :dly (!<!! (delay :dly))))
    (is (= :prm (!<!! (doto (promise) (deliver :prm)))))
    (is (= :cf  (!<!! (CompletableFuture/completedFuture :cf)))))
  (testing "reading a plain-valued ->async-reader via alts! commits the shared
            handler, leaving no phantom taker on the losing (parked) port"
    (let [ch (a/chan)
          reader (f/->async-reader 42)
          [v port] (a/alts!! [ch reader])]
      (is (= 42 v))
      (is (identical? reader port) "the ready reader must win the alts")
      (is (nil? (a/offer! ch :x))
          "losing port must have no phantom taker after alts! resolves"))))

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

(deftest ->executor-service-test
  (testing "ExecutorService is returned identically (no double-wrapping)"
    (let [es (Executors/newSingleThreadExecutor)]
      (try
        (is (identical? es (impl/->executor-service es)))
        (finally
          (.shutdown es)))))
  (testing "plain Executor is wrapped and routes execute to the underlying executor"
    (let [calls (atom 0)
          last-runnable (atom nil)
          ^Executor e (reify Executor
                        (execute [_ r]
                          (swap! calls inc)
                          (reset! last-runnable r)
                          (.run r)))
          es (impl/->executor-service e)
          ran (atom false)]
      (is (instance? ExecutorService es))
      (is (not (identical? e es)))
      (.execute es ^Runnable #(reset! ran true))
      (is (= 1 @calls))
      (is (true? @ran))
      (is (some? @last-runnable))))
  (testing "submit on the wrapper routes through the underlying executor and returns a Future"
    (let [calls (atom 0)
          ^Executor e (reify Executor
                        (execute [_ r]
                          (swap! calls inc)
                          (.run r)))
          es (impl/->executor-service e)
          fut (.submit es ^Callable (fn [] ::done))]
      (is (= 1 @calls))
      (is (= ::done (.get fut)))))
  (testing "lifecycle methods throw UnsupportedOperationException on the wrapper"
    (let [^Executor e (reify Executor (execute [_ r] (.run r)))
          es (impl/->executor-service e)]
      (is (thrown? UnsupportedOperationException (.shutdown es)))
      (is (thrown? UnsupportedOperationException (.shutdownNow es)))
      (is (thrown? UnsupportedOperationException (.isShutdown es)))
      (is (thrown? UnsupportedOperationException (.isTerminated es)))
      (is (thrown? UnsupportedOperationException
                   (.awaitTermination es 1 TimeUnit/MILLISECONDS)))))
  (testing "lifecycle methods on a real ExecutorService pass through unchanged"
    (let [es (Executors/newSingleThreadExecutor)
          wrapped (impl/->executor-service es)]
      (is (false? (.isShutdown wrapped)))
      (.shutdown wrapped)
      (is (true? (.isShutdown wrapped)))
      (is (true? (.awaitTermination wrapped 1 TimeUnit/SECONDS)))))
  (testing "get-pool returns an ExecutorService for built-in workloads"
    (doseq [workload [:io :mixed :compute]]
      (is (instance? ExecutorService (get-pool workload))
          (str "workload " workload " should yield an ExecutorService")))))

(deftest with-async-factory-test
  (testing "binds *async-factory* inside body and restores it after"
    (let [prior f/*async-factory*
          marker (fn marker-factory [] ::async-marker)
          captured (atom nil)]
      (f/with-async-factory marker
        (reset! captured f/*async-factory*))
      (is (identical? marker @captured))
      (is (= ::async-marker (@captured)))
      (is (identical? prior f/*async-factory*))))
  (testing "f/async-factory inside the body uses the bound factory"
    (let [marker (fn [] ::async-marker)]
      (f/with-async-factory marker
        (is (= ::async-marker (f/async-factory))))))
  (testing "thread-factory falls back to *async-factory* when *thread-factory* is unbound"
    (let [marker (fn [] ::async-marker)]
      (f/with-async-factory marker
        (binding [f/*thread-factory* nil]
          (is (= ::async-marker (f/thread-factory)))))))
  (testing "nested with-async-factory rebinds and unwinds correctly"
    (let [outer (fn [] ::outer)
          inner (fn [] ::inner)]
      (f/with-async-factory outer
        (is (= ::outer (f/async-factory)))
        (f/with-async-factory inner
          (is (= ::inner (f/async-factory))))
        (is (= ::outer (f/async-factory))))))
  (testing "nil binding is honored and async-factory falls back to async-promise-factory default"
    (f/with-async-factory nil
      (binding [f/*thread-factory* nil]
        (is (f/async? (f/async-factory))))))
  (testing "exceptions in body still restore the prior binding"
    (let [prior f/*async-factory*]
      (is (thrown? ExceptionInfo
                   (f/with-async-factory (fn [] ::oops)
                     (throw (ex-info "boom" {})))))
      (is (identical? prior f/*async-factory*)))))

(deftest with-thread-factory-test
  (testing "binds *thread-factory* inside body and restores it after"
    (let [prior f/*thread-factory*
          marker (fn marker-factory [] ::thread-marker)
          captured (atom nil)]
      (f/with-thread-factory marker
        (reset! captured f/*thread-factory*))
      (is (identical? marker @captured))
      (is (= ::thread-marker (@captured)))
      (is (identical? prior f/*thread-factory*))))
  (testing "f/thread-factory inside the body uses the bound factory and takes precedence over *async-factory*"
    (let [t-marker (fn [] ::thread-marker)
          a-marker (fn [] ::async-marker)]
      (f/with-async-factory a-marker
        (f/with-thread-factory t-marker
          (is (= ::thread-marker (f/thread-factory)))))))
  (testing "*async-factory* is unaffected by with-thread-factory"
    (let [prior-async f/*async-factory*]
      (f/with-thread-factory (fn [] ::thread-marker)
        (is (identical? prior-async f/*async-factory*)))))
  (testing "nested with-thread-factory rebinds and unwinds correctly"
    (let [outer (fn [] ::outer)
          inner (fn [] ::inner)]
      (f/with-thread-factory outer
        (is (= ::outer (f/thread-factory)))
        (f/with-thread-factory inner
          (is (= ::inner (f/thread-factory))))
        (is (= ::outer (f/thread-factory))))))
  (testing "exceptions in body still restore the prior binding"
    (let [prior f/*thread-factory*]
      (is (thrown? ExceptionInfo
                   (f/with-thread-factory (fn [] ::oops)
                     (throw (ex-info "boom" {})))))
      (is (identical? prior f/*thread-factory*)))))
