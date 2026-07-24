(ns ^:no-doc futurama.impl
  (:require
   [clojure.core.async :refer [take!] :as async]
   [clojure.core.async.impl.go :as go-impl]
   [clojure.core.async.impl.channels :refer [box]]
   [clojure.core.async.impl.protocols :as core-impl]
   [clojure.core.async.impl.ioc-macros :as rt])
  (:import
   [clojure.lang
    Var]
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

(defn- async-reader-handler*
  [cb val]
  (if (async? val)
    (take! val (partial async-reader-handler* cb))
    (cb val)))

(defn async-reader-handler
  [cb]
  (partial async-reader-handler* cb))

(defn async-read-port-take!
  [x handler]
  (let [^Lock handler handler
        commit-handler (fn do-commit []
                         (.lock handler)
                         (let [take-cb (and (core-impl/active? handler)
                                            (core-impl/commit handler))]
                           (.unlock handler)
                           take-cb))]
    (when-let [cb (commit-handler)]
      (cond
        (completed? x)
        (let [r (get! x)]
          (if (async? r)
            (do
              (take! r (async-reader-handler cb))
              nil)
            (box r)))

        :else
        (do
          (on-complete x cb)
          nil)))))

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

;;; Custom parking terminators to snapshot the thread binding frame before registering the callback,
;;; so that a resume on another thread sees the correct frame.

(defn ioc-take!
  "Calls core.async's ioc-take!, but snapshots the thread binding frame before registering the callback."
  [state blk c]
  (rt/aset-object state rt/BINDINGS-IDX (Var/getThreadBindingFrame))
  (rt/take! state blk c))

(defn ioc-put!
  "Calls core.async's ioc-put!, but snapshots the thread binding frame before registering the callback."
  [state blk c val]
  (rt/aset-object state rt/BINDINGS-IDX (Var/getThreadBindingFrame))
  (rt/put! state blk c val))

(defn ioc-alts!
  "Calls core.async's ioc-alts!, but snapshots the thread binding frame before registering the callback."
  [state cont-block ports & opts]
  (rt/aset-object state rt/BINDINGS-IDX (Var/getThreadBindingFrame))
  (apply async/ioc-alts! state cont-block ports opts))

(def async-custom-terminators
  "Custom parking terminators to snapshot the thread binding frame before registering the callback."
  (assoc rt/async-custom-terminators
         `async/<! `ioc-take!
         `async/>! `ioc-put!
         `async/alts! `ioc-alts!))
