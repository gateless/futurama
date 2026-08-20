(ns ^:no-doc futurama.impl
  (:require
   [clojure.core.async :as async]
   [clojure.core.async.impl.go :as go-impl]
   [clojure.core.async.impl.channels :refer [box]]
   [clojure.core.async.impl.protocols :as core-impl])
  (:import
   [java.util.concurrent
    AbstractExecutorService
    ExecutorService
    Executor
    Future
    FutureTask]
   [java.util.concurrent.locks Lock]
   [java.util.function BiConsumer]))

(def async-state-machine
  go-impl/state-machine)

(defprotocol AsyncCompletableReader
  (get! [x]
    "Returns the completed value of this async operation, blocking if the async operation is not yet complete.")
  (completed? [x]
    "Returns true if this async operation has completed.")
  (on-complete [x f]
    "Registers a callback f to be called with the completed value when this async operation completes."))

(defprotocol AsyncCompletableWriter
  (complete! [x v]
    "Attempts to complete this async operation with value v, returning true if successful, false otherwise."))

(defprotocol AsyncCancellable
  (on-cancel-interrupt [this fut]
    "Attempts to register a cancellation handler that will be called with the given future when this async operation is cancelled.")
  (cancelled? [this]
    "Returns true if this async operation has been cancelled.")
  (cancel! [this]
    "Attempts to cancel this async operation."))

(defmacro async?
  "Returns true if the given value satisfies core.async's `ReadPort`."
  [x]
  `(satisfies? core-impl/ReadPort ~x))

(defmacro async-channel?
  "Returns true if the given value satisfies core.async's `Channel`."
  [x]
  `(satisfies? core-impl/Channel ~x))

(defmacro async-completable-writer?
  "Returns true if the given value is an async operation that can be completed (i.e., satisfies AsyncCompletableWriter)."
  [x]
  `(satisfies? AsyncCompletableWriter ~x))

(defmacro async-completable-reader?
  "Returns true if the given value is an async operation that can be read (i.e., satisfies AsyncCompletableReader)."
  [x]
  `(satisfies? AsyncCompletableReader ~x))

(defmacro async-cancellable?
  "Determines if v can be cancelled"
  [x]
  `(satisfies? AsyncCancellable ~x))

(defn ->executor-service
  "Returns the input unchanged when it is already an ExecutorService; otherwise wraps it in
  a proxy that forwards `execute` to the underlying Executor.

  The wrapper exists because `get-pool` historically returned an ExecutorService and downstream
  consumers (notably `core.async/thread`, which calls `.submit` on the pool) depend on that
  contract. core.async 1.9's `executor-for` now returns a plain Executor, so without this
  adapter those callers break.

  Lifecycle methods (`shutdown`, `shutdownNow`, `isShutdown`, `isTerminated`, `awaitTermination`)
  throw UnsupportedOperationException: this proxy does not own the underlying executor and
  cannot honestly answer for its lifecycle. Callers that need to manage a pool's lifecycle
  should hold onto and operate on the original Executor reference, not this wrapper."
  ^ExecutorService [^Executor executor]
  (if (instance? ExecutorService executor)
    executor
    (proxy [AbstractExecutorService] []
      (execute [^Runnable command]
        (.execute executor command))

      (shutdown []
        (throw (UnsupportedOperationException.
                "shutdown not supported on a non-owning ExecutorService proxy")))

      (shutdownNow []
        (throw (UnsupportedOperationException.
                "shutdownNow not supported on a non-owning ExecutorService proxy")))

      (isShutdown []
        (throw (UnsupportedOperationException.
                "isShutdown not supported on a non-owning ExecutorService proxy")))

      (isTerminated []
        (throw (UnsupportedOperationException.
                "isTerminated not supported on a non-owning ExecutorService proxy")))

      (awaitTermination [_wait-timeout _wait-unit]
        (throw (UnsupportedOperationException.
                "awaitTermination not supported on a non-owning ExecutorService proxy"))))))

(deftype JavaBiConsumer [f]
  BiConsumer
  (accept [_ a b]
    (f a b)))

(defn async-dispatch-task-handler
  "Dispatches a task to the given executor service pool, and registers a cancellation handler on the port."
  ^Future [^Executor pool port ^Runnable task]
  (let [fut (FutureTask. ^Runnable task nil)]
    (.execute pool fut)
    (on-cancel-interrupt port fut)
    port))

(defn delegating-handler [^Lock handler around-callback]
  (reify
    Lock
    (lock [_] (.lock handler))
    (unlock [_] (.unlock handler))
    core-impl/Handler
    (active? [_] (core-impl/active? handler))
    (blockable? [_] (core-impl/blockable? handler))
    (lock-id [_] (core-impl/lock-id handler))
    (commit [_]
      (around-callback (core-impl/commit handler)))))

(def get-pool (delay
                (when-some [v (ns-resolve 'futurama.core 'get-pool)]
                  @v)))

(defn async-read-port-take!
  "Shared `ReadPort/take!` implementation, supports three types of
  async values:
  - an `AsyncCompletableReader` (futurama's completable types): use
    the `completed?`/`get!` synchronous fast-path, otherwise register
    via `on-complete`.
  - a plain core.async `ReadPort` (e.g. a core.async channel):
    supports \"fast-resume\" reads from the channel as a fast path,
    otherwise adds a wrapped version of the handler via the take!
    function of the ReadPort protocol. The wrapping is to support
    recursive unrolling.
  - anything else: box the value directly if fast-resume? is selected,
    otherwise pass to callback from handler.
  
  If a value is read from x the handler is always commited. This
  function delays committing as long as possible when unrolling nested
  asyncs. Still will very likely get weird behavior if you mix nested
  asyncs with alts.

  Metadata on the callback in the handler is preserved and copied if a
  new handler needs to be created when unnesting. This preserves
  behavior with handler callbacks marked as on-caller.

  This function always handler the fast-resume case when reading from
  a channel. The fast-resume? argument controls if the caller of this
  function supports fast-resume. Most futurama impls of the ReadPort
  protocol that call this function don't support the fast-resume part
  of the ReadPort protcol since they were originally written against
  the public take! function in core.async which doesn't expose that
  part of the ReadPort protocol to callers."
  ([x handler]
   (async-read-port-take! x handler true))
  ([x ^Lock handler fast-resume?]
   (cond (and (async-completable-reader? x) (completed? x))
         (recur (get! x) handler fast-resume?)
         (async-completable-reader? x)
         (do
           (on-complete x (fn [result] (async-read-port-take! result handler false)))
           nil)
         (async? x)
         (when-some [result (->> (fn [take-cb]
                                   ;; copy metadata to
                                   ;; propagate :on-caller setting
                                   (with-meta
                                     (fn [value]
                                       (async-read-port-take!
                                        value
                                        (async/fn-handler
                                         take-cb
                                         (core-impl/blockable? handler))
                                        ;; when running in anything
                                        ;; other than the first level
                                        ;; callback, we can't support
                                        ;; fast resume, because we
                                        ;; can't be sure we are likely
                                        ;; not running on the same
                                        ;; thread.
                                        false))
                                     (meta take-cb)))
                                 (delegating-handler handler)
                                 (core-impl/take! x))]
           (let [_ (.lock handler)
                 ;; in this fast-resume case active? will always be
                 ;; true, but a pattern is a pattern
                 take-cb (and (core-impl/active? handler)
                              (core-impl/commit handler))
                 _ (.unlock handler)]
             (when take-cb
               (recur @result (async/fn-handler take-cb (core-impl/blockable? handler)) fast-resume?))))
         :else
         (let [_ (.lock handler)
               take-cb (and (core-impl/active? handler)
                            (core-impl/commit handler))
               _ (.unlock handler)]
           (when take-cb
             (if fast-resume?
               (box x)
               (do
                 (if (:on-caller (meta take-cb))
                    (take-cb x)
                    (if-some [gp @get-pool]
                      (.execute ^Executor (gp :mixed) #(take-cb x))
                      ;; fallback to runnning on the same thread if failed
                      ;; to find the pool for some reason, can cause
                      ;; non-channel asyncs to stackoverflow, particularly
                      ;; deferreds.
                      (take-cb x)))
                 nil)))))))

(defn async-write-port-put!
  [x val handler]
  (when (nil? val)
    (throw (IllegalArgumentException. "Can't put nil on an async thing, close it instead!")))
  (let [^Lock handler handler]
    (if (and (async-completable-reader? x)
             (completed? x))
      (do
        (.lock handler)
        (when (core-impl/active? handler)
          (core-impl/commit handler))
        (.unlock handler)
        (box false))
      (do
        (.lock handler)
        (when (core-impl/active? handler)
          (core-impl/commit handler))
        (.unlock handler)
        (box
         (complete! x val))))))
