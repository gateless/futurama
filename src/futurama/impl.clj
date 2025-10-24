(ns ^:no-doc futurama.impl
  (:refer-clojure :exclude [realized?])
  (:require
   [clojure.core.async :refer [take!]]
   [clojure.core.async.impl.go :as go-impl]
   [clojure.core.async.impl.channels :refer [box]]
   [clojure.core.async.impl.protocols :as core-impl])
  (:import
   [java.util.concurrent
    ExecutorService
    Future]
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

(deftype JavaBiConsumer [f]
  BiConsumer
  (accept [_ a b]
    (f a b)))

(defn async-dispatch-task-handler
  "Dispatches a task to the given executor service pool, and registers a cancellation handler on the port."
  ^Future [^ExecutorService pool port ^Runnable task]
  (let [^Future fut (.submit pool task)]
    (on-cancel-interrupt port fut)
    port))

(defn- async-reader-handler*
  [cb val]
  (if (satisfies? core-impl/ReadPort val)
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
          (if (satisfies? core-impl/ReadPort r)
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
    (if (and (satisfies? AsyncCompletableReader x)
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
