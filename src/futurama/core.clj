(ns futurama.core
  "Futurama is a Clojure library for more deeply integrating async abstractions in the Clojure and JVM ecosystem with Clojure.

  async and thread blocks are dispatched over an internal thread pool,
  which defaults to using the `ForkJoinPool/commonPool`. They work very
  similarly to core.async go blocks and thread blocks. Notable different
  ways are that, async and thread blocks will return Throwable values when
  exceptions are uncaught, and that taking a value from an async result
  using `!<!` or `!<!!` will reduce nested async values to a single result.

  The dynamic variables `*async-factory*` and `*thread-factory*` can be bound
  to control which async result type is returned when the `async` or `thread`
  macros are used. If an `*async-factory*` is bound but no `*thread-factory*`,
  then calls to `thread` will use the `*async-factory*`, or if a factory is not
  defined then the default `async-promise-factory` will be used instead.

  Possible factories supported are:
  - `async-channel-factory`: creates a core.async channel result, with a buffer of size 1.
  - `async-promise-factory`: creates a core.async promise-channel result.
  - `async-deferred-factory`: creates a manifold deferred result.
  - `async-future-factory`: creates a CompletableFuture result.

  Use the Java system property `futurama.executor-factory`
  to specify a function that will provide ExecutorServices for
  application-wide use by futurama in lieu of its defaults.
  To ensure that futurama uses the same thread pool as core.async,
  you can set the property to the value `clojure.core.async.impl.dispatch/executor-for`.
  The property value should name a fully qualified var. The function
  will be passed a keyword indicating the context of use of the
  executor, and should return either an ExecutorService, or nil to
  use the default. Results per keyword will be cached and used for
  the remainder of the application.

  Possible context arguments are:

  :io - used for :io workloads, default workload for `async` dispatch, use via `(thread :io ...)` or `(async :io ...)`.
  :mixed - used for :mixed workloads, default workload for `thread` dispatch.
  :compute - used for :compute workloads, use via `(thread :compute ...)` or `(async :compute ...)`.

  The set of contexts may grow in the future so the function should
  return nil for unexpected contexts.

  Copied from core.async, because this is super useful:

  Set Java system property `clojure.core.async.go-checking` to true
  to validate async blocks do not invoke core.async blocking operations.
  Property is read once, at namespace load time. Recommended for use
  primarily during development. Invalid blocking calls will throw in
  go block threads - use Thread.setDefaultUncaughtExceptionHandler()
  to catch and handle."
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.dispatch :as run-impl]
            [clojure.core.async.impl.protocols :as core-impl]
            [clojure.core.async.impl.channels :refer [box]]
            [clojure.core.async.impl.ioc-macros :as rt]
            [clojure.core.reducers :as r]
            [futurama.impl :as impl]
            [futurama.state :as state]
            [manifold.deferred :as d])
  (:import [clojure.lang Var IDeref IPending IFn IAtom IRef]
           [clojure.core.async.impl.channels ManyToManyChannel]
           [clojure.core.async.impl.buffers PromiseBuffer]
           [java.util.concurrent
            CompletableFuture
            CompletionException
            ExecutionException
            CancellationException
            ExecutorService
            ForkJoinPool
            Future]
           [java.util.concurrent.locks Lock]
           [java.util.function BiConsumer]
           [manifold.deferred Deferred]))

(def ^:const ASYNC_CANCELLED ::cancelled)

(def ^:dynamic *thread-pool* nil)

(defn async-channel-factory
  "Creates a core async channel of size 1"
  ^ManyToManyChannel []
  (async/chan 1 nil identity))

(defn async-promise-factory
  "Creates a core async promise channel"
  ^ManyToManyChannel []
  (async/promise-chan nil identity))

(defn async-future-factory
  "Creates a new CompletableFuture"
  ^CompletableFuture []
  (CompletableFuture.))

(defn async-deferred-factory
  "Creates a new Manifold Deferred"
  ^Deferred []
  (d/deferred))

(def ^:dynamic *async-factory* nil)

(def ^:dynamic *thread-factory* nil)

(defn set-async-factory!
  "alters the root binding of `*async-factory*` to be equal to `async-factory-fn`"
  [async-factory-fn]
  (alter-var-root #'*async-factory* (constantly async-factory-fn)))

(defn set-thread-factory!
  "alters the root binding of `*thread-factory*` to be equal to `thread-factory-fn`"
  [thread-factory-fn]
  (alter-var-root #'*thread-factory* (constantly thread-factory-fn)))

(defmacro with-async-factory
  "temporarily binds `*async-factory*` to the specified `async-factory-fn` and executes body."
  [async-factory-fn & body]
  `(binding [*async-factory* ~async-factory-fn]
     ~@body))

(defmacro with-thread-factory
  "temporarily binds `*thread-factory*` to the specified `thread-factory-fn` and executes body."
  [thread-factory-fn & body]
  `(binding [*thread-factory* ~thread-factory-fn]
     ~@body))

(defn async-factory
  "builds a result object to be used inside async! macro, this function uses the `*async-factory*`
  if it is bound, or the default factory of `async-promise-factory`."
  []
  (cond
    *async-factory*
    (*async-factory*)

    :else
    (async-promise-factory)))

(defn thread-factory
  "builds a result object to be used inside thread! macro, this function uses the first available
  factory of `*thread-factory*` or `*async-factory*`, or if neither is bound then uses the default
  factory of `async-future-factory`."
  []
  (cond
    *thread-factory*
    (*thread-factory*)

    *async-factory*
    (*async-factory*)

    :else
    (async-future-factory)))

(def get-pool
  "Given a workload tag, returns an ExecutorService instance and memoizes the result.
  By default, futurama will defer to a user factory (if provided via sys prop)
  or the `ForkJoinPool/commonPool` instance. When using core.async 1.7 or higher it's possible to
  set the `futurama.executor-factory` property to `clojure.core.async.impl.dispatch/executor-for`
  and futurama will use the same pools as core.async."
  (memoize
   (fn ^ExecutorService [workload]
     (let [sysprop-factory (when-let [esf (System/getProperty "futurama.executor-factory")]
                             (requiring-resolve (symbol esf)))
           sp-exec (and sysprop-factory (sysprop-factory workload))]
       (or sp-exec
           (ForkJoinPool/commonPool))))))

(defmacro with-pool
  "Utility macro which binds *thread-pool* to the supplied pool and then evaluates the `body`.
  The `pool` can be an ExecutorService or can also be a keyword/string to be passed to the `futurama.executor-factory`"
  [pool & body]
  `(let [pool# ~pool]
     (binding [*thread-pool* (if (keyword? pool#)
                               (get-pool pool#)
                               pool#)]
       ~@body)))

(defn unwrap-exception
  "unwraps an ExecutionException or CompletionException via ex-cause until the root exception is returned"
  ^Throwable [^Throwable ex]
  (if-let [ce (and (or (instance? ExecutionException ex)
                       (instance? CompletionException ex))
                   (ex-cause ex))]
    ce
    ex))

(defn rethrow-exception
  "throw v if it is an Exception"
  [v]
  (if (instance? Throwable v)
    (throw (unwrap-exception v))
    v))

(def ^:no-doc rte rethrow-exception)

(defn- set-cancel-state!
  [x]
  (state/set-cancel-state! x ASYNC_CANCELLED))

(defn- get-cancel-state
  [x]
  (= (state/get-cancel-state x) ASYNC_CANCELLED))

(defn async-cancellable?
  "Determines if v can be cancelled"
  [v]
  (satisfies? impl/AsyncCancellable v))

(defn async-cancel!
  "Cancels the async item."
  [item]
  (when (async-cancellable? item)
    (impl/cancel! item)))

(defn async-cancelled?
  "Checks if the current executing async item or one of its parents or provided item has been cancelled.
  Also checks if the thread has been interrupted and restores the interrupt status."
  ([]
   (or (when (Thread/interrupted)
         (.. (Thread/currentThread)
             (interrupt))
         true)
       (some async-cancelled? state/*items*)
       false))
  ([item]
   (when (satisfies? impl/AsyncCancellable item)
     (impl/cancelled? item))))

(defn async-completed?
  "Checks if the provided `AsyncCompletableReader` instance is completed?"
  [x]
  (cond
    (satisfies? impl/AsyncCompletableReader x)
    (impl/completed? x)

    (satisfies? core-impl/Channel x)
    (core-impl/closed? x)

    :else false))

(defn async?
  "returns true if v instance satisfies? core.async's `ReadPort`"
  ^Boolean [v]
  (satisfies? core-impl/ReadPort v))

(defmacro thread!
  "Asynchronously invokes the body inside a pooled thread and return over a write port,
  preserves the current thread binding frame, the pool used can be specified via `*thread-pool*`."
  [pool port & body]
  `(let [pool# (or ~pool *thread-pool* (get-pool :mixed))
         port# ~port]
     (state/push-item port#
       (let [binding-frame# (Var/cloneThreadBindingFrame)]
         (impl/async-dispatch-task-handler pool# port#
           (^:once fn* []
             (let [thread-frame# (Var/getThreadBindingFrame)]
               (Var/resetThreadBindingFrame binding-frame#)
               (try
                 (let [~'res (do ~@body)]
                   (when (some? ~'res)
                     (async/>!! port# ~'res)))
                 (catch Throwable ~'e
                   (async/>!! port# (unwrap-exception ~'e)))
                 (finally
                   (async/close! port#)
                   (Var/resetThreadBindingFrame thread-frame#))))))))))

(defmacro thread
  "Asynchronously invokes the body in a pooled thread, preserves the current thread binding frame,
  and returns the value in a port created via `thread-factory`, the pool used can be specified
  via `*thread-pool*`, or through a keyword :io, :mixed, :compute for example."
  [& workload-and-body]
  (let [[workload & body] (if (and (keyword? (first workload-and-body))
                                   (seq (rest workload-and-body)))
                            workload-and-body
                            (cons nil workload-and-body))
        thread-pool (if workload
                      `(get-pool ~workload)
                      `*thread-pool*)]
    `(thread!
       ~thread-pool
       (thread-factory)
       ~@body)))

(defmacro async!
  "Asynchronously executes the body, returning `port` immediately to the
  calling thread. Additionally, any visible calls to !<!, <!, >! and alt!/alts!
  channel operations within the body will block (if necessary) by
  'parking' the calling thread rather than tying up an OS thread.
  Upon completion of the operation, the body will be resumed.

  The success or failure output will be put! in `port`.

  async blocks should not (either directly or indirectly) perform operations
  that may block indefinitely. Doing so risks depleting the fixed pool of
  go block threads, causing all go block processing to stop. This includes
  core.async blocking ops (those ending in !!) and other blocking IO.

  Returns the provided port which will receive the result of the body when
  completed; the pool used can be specified via `*thread-pool*` binding."
  [pool port & body]
  (let [crossing-env (zipmap (keys &env) (repeatedly gensym))]
    `(let [pool# (or ~pool *thread-pool* (get-pool :io))
           port# ~port]
       (state/push-item port#
         (let [captured-bindings# (Var/getThreadBindingFrame)]
           (impl/async-dispatch-task-handler pool# port#
             (^:once fn* []
               (run-impl/with-dispatch-thread-marking
                 (let [~@(mapcat
                          (fn [[l sym]]
                            [sym `(^:once fn* []
                                    ~(vary-meta l dissoc :tag))])
                          crossing-env)
                       f# ~(impl/async-state-machine
                            `(try
                               ~@body
                               (catch Throwable ~'e
                                 (unwrap-exception ~'e))) 1 [crossing-env &env] rt/async-custom-terminators)
                       state# (-> (f#)
                                  (rt/aset-all! rt/USER-START-IDX port#
                                                rt/BINDINGS-IDX captured-bindings#))]
                   (rt/run-state-machine-wrapped state#))))))))))

(defmacro async
  "Asynchronously executes the body, returning immediately to the
  calling thread. Additionally, any visible calls to !<!, <!, >! and alt!/alts!
  channel operations within the body will block (if necessary) by
  'parking' the calling thread rather than tying up an OS thread.
  Upon completion of the operation, the body will be resumed.

  async blocks should not (either directly or indirectly) perform operations
  that may block indefinitely. Doing so risks depleting the fixed pool of
  go block threads, causing all go block processing to stop. This includes
  core.async blocking ops (those ending in !!) and other blocking IO.

  Returns an instance of the `(async-factory)` which will receive the result of the body when
  completed; the pool used can be specified via `*thread-pool*` binding."
  [& workload-and-body]
  (let [[workload & body] (if (and (keyword? (first workload-and-body))
                                   (seq (rest workload-and-body)))
                            workload-and-body
                            (cons nil workload-and-body))
        thread-pool (if workload
                      `(get-pool ~workload)
                      `*thread-pool*)]
    `(async!
       ~thread-pool
       (async-factory)
       ~@body)))

(deftype AsyncReader [val]
  core-impl/ReadPort
  (take! [_ handler]
    (let [^Lock handler handler
          commit-handler (fn do-commit []
                           (.lock handler)
                           (let [take-cb (and (core-impl/active? handler) (core-impl/commit handler))]
                             (.unlock handler)
                             take-cb))]
      (when-let [cb (commit-handler)]
        (if (async? val)
          (do
            (async/take! val (impl/async-reader-handler cb))
            nil)
          (box val))))))

(defn ->async-reader
  "Creates an AsyncReader to read anything via `take!`"
  [x]
  (AsyncReader. x))

(def ^:no-doc rdr ->async-reader)

(defmacro <!
  "An improved macro version of <!, which rethrows exceptions returned over the channel.
  Must be called INSIDE a (go ...) or (async ...) block.
  - Will return nil if closed.
  - Will park if nothing is available.
  - Will throw if an Exception is taken from port.
  - Will return the raw value if it is not a ReadPort"
  [v]
  `(rte (async/<! ~v)))

(defmacro <!*
  "Like <! but works with collections of async values"
  [coll]
  `(loop [~'icoll (not-empty ~coll)
          ~'ocoll []]
     (if (nil? ~'icoll)
       ~'ocoll
       (recur (next ~'icoll) (conj ~'ocoll (<! (first ~'icoll)))))))

(defmacro <!!
  "An improved macro version of <!!, which also rethrows exceptions returned over the channel.
  Must be called OUTSIDE a (go ...) or (async ...) block.
  - Will return nil if closed.
  - Will block if nothing is available.
  - Will throw if a Exception is taken from port.
  - Will return the raw value if it is not a ReadPort"
  [v]
  `(rte (async/<!! ~v)))

(defmacro !<!
  "An improved macro version of <!, which also rethrows exceptions returned over the channel.
  Must be called INSIDE a (go ...) or (async ...) block.
  - Will return nil if closed.
  - Will park if nothing is available.
  - Will throw if an Exception is taken from port.
  - Will return the raw value if it is not a ReadPort
  - Will fully read through any async result returned"
  [v]
  `(<! (rdr ~v)))

(defmacro !<!*
  "Like !<! but works with collections of async values"
  [coll]
  `(loop [~'icoll (not-empty ~coll)
          ~'ocoll []]
     (if (nil? ~'icoll)
       ~'ocoll
       (recur (next ~'icoll) (conj ~'ocoll (!<! (first ~'icoll)))))))

(defmacro !<!!
  "An improved macro version of <!!, which also rethrows exceptions returned over the channel.
  Must be called OUTSIDE a (go ...) or (async ...) block.
  - Will return nil if closed.
  - Will block if nothing is available.
  - Will throw if a Exception is taken from port.
  - Will return the raw value if it is not a ReadPort
  - Will fully read through any async result returned"
  [v]
  `(<!! (rdr ~v)))

(defmacro async-for
  "works like a for macro, but supports core.async operations.
  bindings are initially bound and collected, then the data is iterated
  over using a loop and the output collected; note that only the body of
  the for can contain `<!` and `!<!` calls. This is implicitly wrapped in
  an `async` block."
  [bindings & body]
  (let [pairs (partition 2 bindings)
        bvars (loop [[pair & more] pairs
                     vars []]
                (if (nil? pair)
                  (vec (set vars))
                  (let [[pk pv] pair
                        [vars pairs]
                        (if (= pk :let)
                          [vars
                           (concat more (partition 2 pv))]
                          [(reduce
                            (fn flatten-reducer
                              [vs v]
                              (cond
                                (symbol? v)
                                (conj vs v)

                                (coll? v)
                                (into vs (reduce flatten-reducer [] v))

                                :else
                                vs))
                            vars
                            [pk])
                           more])]
                    (recur pairs vars))))]
    `(async
       (let [args# (for [~@bindings]
                     [~@bvars])
             output#
             (loop [results# []
                    args# (seq args#)]
               (if (nil? args#)
                 results#
                 (recur (conj results#
                              (let [[~@bvars] (first args#)]
                                ~@body))
                        (next args#))))]
         (!<!* output#)))))

(defn async-map
  "Asynchronously returns the result of applying f to
  the set of first items of each coll, followed by applying f to the
  set of second items in each coll, until any one of the colls is
  exhausted.  Any remaining items in other colls are ignored. Function
  f should accept number-of-colls arguments."
  [f coll & coll-seq]
  (async-for
   [params (let [colls (!<!* (cons coll coll-seq))]
             (->> (apply interleave colls)
                  (partition (count colls))))]
   (apply f params)))

(defn- async-do-reduce*
  "internal reducer function to work with async args"
  [f & args]
  (async
    (apply f (!<!* args))))

(defn async-reduce
  "Like core/reduce except, when init is not provided, (f) is used, and async results are read with !<!."
  ([f coll]
   (async-reduce f (f) coll))
  ([f init coll]
   (async
     (r/reduce (partial async-do-reduce* f)
               (!<! init)
               (!<! coll)))))

(defn- async-some-call*
  "internal async-some fn handler to handle async args and result"
  [result pred item]
  (async
    (when-let [value (!<! (pred (!<! item)))]
      (async/put! result value))))

(defn async-some
  "Concurrently executes (pred x) and returns the first returned
  logical true value of (pred x) for any x in coll, else nil.
  One common idiom is to use a set as pred, for example
  this will return :fred if :fred is in the sequence, otherwise nil:
  (some #{:fred} coll). Please beware that unlike `clojure.core/some`
  this function returns the first asynchronous result that completes
  and evaluates to logical true, but not necessarily the first one
  in sequential order."
  [pred coll]
  (let [result (async-factory)]
    (async!
      *thread-pool*
      result
      (let [results (doall
                     (for [item (!<! coll)]
                       (async-some-call* result pred item)))]
        (loop [results (seq results)]
          (cond
            (core-impl/closed? result)
            nil

            (nil? results)
            (async/close! result)

            :else
            (do
              (!<! (first results))
              (recur (next results)))))))
    result))

(defn- async-every-call*
  "internal async-every? fn handler to handle async args and result"
  [result pred item]
  (async
    (when-not (!<! (pred (!<! item)))
      (async/put! result false))))

(defn async-every?
  "Returns true if (pred x) is logical true for every x in coll, else false."
  [pred coll]
  (let [result (async-factory)]
    (async!
      *thread-pool*
      result
      (let [results (for [item coll]
                      (async-every-call* result pred item))]
        (loop [results (seq results)]
          (cond
            (core-impl/closed? result)
            nil

            (nil? results)
            (async/put! result true)

            :else
            (do
              (!<! (first results))
              (recur (next results)))))))
    result))

(defmacro async->
  "Threads the expr through the forms which can return async result.
  Inserts v as the second item in the first form, making a list of it
  if it is not a list already. If there are more forms, inserts the
  first form as the second item in second form, etc."
  [v & call-seq]
  (let [call-parsed-seq (for [call call-seq]
                          (if (list? call)
                            (vec call)
                            (vector call)))]
    `(async
       (loop [v# ~v
              [call# & more#] [~@call-parsed-seq]]
         (if-not call#
           v#
           (let [arg# (!<! v#)
                 [f# & args#] call#
                 res# (!<! (apply f# arg# args#))]
             (recur res# more#)))))))

(defmacro async->>
  "Threads the expr through the forms which can return async result.
  Inserts x as the last item in the first form, making a list of it
  if it is not a list already. If there are more forms, inserts the
  first form as the last item in second form, etc."
  [v & call-seq]
  (let [call-parsed-seq (for [call call-seq]
                          (if (list? call)
                            (vec call)
                            (vector call)))]
    `(async
       (loop [v# ~v
              [call# & more#] [~@call-parsed-seq]]
         (if-not call#
           v#
           (let [arg# (!<! v#)
                 [f# & args#] call#
                 r# (!<! (apply f# (concat args# [arg#])))]
             (recur r# more#)))))))

(defn- async-walk-record*
  "internal async walk adapter to walk a record"
  [innerf r x]
  (async
    (conj (!<! r) (!<! (innerf (!<! x))))))

(defn async-walk
  "Traverses form, an arbitrary data structure.  inner and outer are
  functions.  Applies inner to each element of form, building up a
  data structure of the same type, then applies outer to the result.
  Recognizes all Clojure data structures. Consumes seqs as with doall."
  [inner outer form]
  (async
    (let [form (!<! form)]
      (cond
        (list? form)
        (!<! (outer (apply list (!<! (async-map inner form)))))

        (instance? clojure.lang.IMapEntry form)
        (outer (clojure.lang.MapEntry/create (!<! (inner (!<! (key form))))
                                             (!<! (inner (!<! (val form))))))

        (seq? form)
        (outer (!<! (async-map inner form)))

        (instance? clojure.lang.IRecord form)
        (outer (!<! (reduce (partial async-walk-record* inner)
                            form form)))

        (coll? form)
        (outer (into (empty form) (!<! (async-map inner form))))

        :else
        (outer form)))))

(defn async-postwalk
  "Performs a depth-first, post-order traversal of form.  Calls f on
  each sub-form, uses f's return value in place of the original.
  Recognizes all Clojure data structures. Consumes seqs as with doall."
  [f form]
  (async-walk (partial async-postwalk f) f form))

(defn async-prewalk
  "Like postwalk, but does pre-order traversal."
  [f form]
  (async-walk (partial async-prewalk f) identity (f form)))

;;; Begin protocol implementations

(extend-type Future
  core-impl/ReadPort
  (take! [x handler]
    (impl/async-read-port-take! x handler))

  impl/AsyncCompletableReader
  (get! [fut]
    (try
      (.get ^Future fut)
      (catch Throwable e
        e)))
  (completed? [fut]
    (.isDone ^Future fut))
  (on-complete [fut f]
    (async/thread
      (let [r (impl/get! fut)]
        (f r)))
    fut)

  impl/AsyncCancellable
  (cancel! [fut]
    (set-cancel-state! fut)
    (future-cancel fut))
  (on-cancel-interrupt [fut _]
    fut)
  (cancelled? [fut]
    (or (future-cancelled? fut)
        (get-cancel-state fut)))

  core-impl/Channel
  (close! [fut]
    (when-not (impl/completed? fut)
      (impl/cancel! fut))
    nil)
  (closed? [fut]
    (impl/completed? fut)))

(extend-protocol impl/AsyncCompletableWriter
  IFn
  (complete! [f v]
    (boolean (f v)))

  IAtom
  (complete! [a v]
    (reset! a v)
    true)

  IRef
  (complete! [a v]
    (dosync
     (ref-set a v))
    true))

(extend-protocol core-impl/WritePort
  IFn
  (put! [x val handler]
    (impl/async-write-port-put! x val handler))

  IAtom
  (put! [x val handler]
    (impl/async-write-port-put! x val handler))

  IRef
  (put! [x val handler]
    (impl/async-write-port-put! x val handler)))

(extend-type IDeref
  core-impl/ReadPort
  (take! [x handler]
    (impl/async-read-port-take! x handler))

  impl/AsyncCompletableReader
  (get! [ref]
    (try
      (deref ref)
      (catch Throwable e
        e)))
  (completed? [ref]
    (if (instance? IPending ref)
      (realized? ref)
      true))
  (on-complete [ref f]
    (async/thread
      (let [r (impl/get! ref)]
        (f r)))
    ref)

  impl/AsyncCancellable
  (cancel! [ref]
    (set-cancel-state! ref)
    (if (and (instance? IFn ref)
             (not (impl/completed? ref)))
      (do
        (ref (CancellationException. "Task cancelled"))
        true)
      false))
  (on-cancel-interrupt [ref _]
    ref)
  (cancelled? [ref]
    (get-cancel-state ref))

  core-impl/Channel
  (close! [ref]
    (when (and (instance? IFn ref)
               (not (impl/completed? ref)))
      (ref nil))
    nil)
  (closed? [ref]
    (impl/completed? ref)))

(extend-type CompletableFuture
  core-impl/ReadPort
  (take! [x handler]
    (impl/async-read-port-take! x handler))

  core-impl/WritePort
  (put! [x val handler]
    (impl/async-write-port-put! x val handler))

  impl/AsyncCompletableReader
  (get! [fut]
    (try
      (.get ^CompletableFuture fut)
      (catch Throwable e
        e)))
  (completed? [fut]
    (.isDone ^CompletableFuture fut))
  (on-complete [fut f]
    (let [^BiConsumer invoke-cb (impl/->JavaBiConsumer
                                 (fn [val ex]
                                   (f (or ex val))))]
      (.whenComplete ^CompletableFuture fut ^BiConsumer invoke-cb))
    fut)

  impl/AsyncCompletableWriter
  (complete! [fut v]
    (if (instance? Throwable v)
      (.completeExceptionally ^CompletableFuture fut ^Throwable v)
      (.complete ^CompletableFuture fut v)))

  impl/AsyncCancellable
  (on-cancel-interrupt [fut fut']
    (let [^BiConsumer invoke-cb (impl/->JavaBiConsumer
                                 (fn [_ _]
                                   (when (impl/cancelled? fut)
                                     (future-cancel fut'))))] ;;; cancel any linked future
      (.whenComplete ^CompletableFuture fut ^BiConsumer invoke-cb)
      fut))
  (cancel! [fut]
    (set-cancel-state! fut)
    (future-cancel fut))
  (cancelled? [fut]
    (or (future-cancelled? fut)
        (get-cancel-state fut)))

  core-impl/Channel
  (close! [fut]
    (when-not (impl/completed? fut)
      (.complete ^CompletableFuture fut nil))
    nil)
  (closed? [fut]
    (.isDone ^CompletableFuture fut)))

(extend-type Deferred
  core-impl/ReadPort
  (take! [x handler]
    (impl/async-read-port-take! x handler))

  core-impl/WritePort
  (put! [x val handler]
    (impl/async-write-port-put! x val handler))

  impl/AsyncCompletableReader
  (get! [dfd]
    (try
      (deref dfd)
      (catch Throwable e
        e)))
  (completed? [dfd]
    (d/realized? dfd))
  (on-complete [dfd f]
    (d/on-realized dfd f f)
    dfd)

  impl/AsyncCompletableWriter
  (complete! [dfd v]
    (if (instance? Throwable v)
      (d/error! dfd v)
      (d/success! dfd v)))

  impl/AsyncCancellable
  (cancel! [dfd]
    (set-cancel-state! dfd)
    (if-not (d/realized? dfd)
      (d/error! dfd (CancellationException. "Task cancelled"))
      false))
  (on-cancel-interrupt [dfd fut]
    (let [interrupt-handler (fn [_]
                              (when (impl/cancelled? dfd)
                                (future-cancel fut)))]
      (d/on-realized dfd interrupt-handler interrupt-handler)
      dfd))
  (cancelled? [dfd]
    (get-cancel-state dfd))

  core-impl/Channel
  (close! [dfd]
    (d/success! dfd nil)
    nil)
  (closed? [dfd]
    (d/realized? dfd)))

(extend-type ManyToManyChannel
  impl/AsyncCancellable
  (cancel! [c]
    (set-cancel-state! c)
    (if-not (core-impl/closed? c)
      (do
        (async/put! c (CancellationException. "Task cancelled"))
        (async/close! c)
        true)
      false))
  (on-cancel-interrupt [c fut]
    (when (->> (.buf ^ManyToManyChannel c)
               (instance? PromiseBuffer))
      (async/take! c (fn [_]
                       (when (impl/cancelled? c)
                         (future-cancel fut)))))
    c)
  (cancelled? [c]
    (get-cancel-state c)))

(defn ->future
  "Converts any value into a CompletableFuture, reading
  through any async result returned inside or returning
  a completed future for non-async objects."
  [val]
  (cond
    (instance? CompletableFuture val)
    val

    (async? val)
    (let [fut (CompletableFuture.)]
      (async
        (try
          (let [res (!<! val)]
            (impl/complete! fut res))
          (catch Throwable rex
            (impl/complete! fut rex))))
      fut)

    :else
    (CompletableFuture/completedFuture val)))
