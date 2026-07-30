[![Build Status](https://github.com/gateless/futurama/actions/workflows/clojure.yml/badge.svg)](https://github.com/gateless/futurama/actions/workflows/clojure.yml)

# Futurama

Futurama is a Clojure library that deeply integrates diverse async abstractions across the Clojure and JVM ecosystem with [core.async](https://github.com/clojure/core.async).

It provides a unified interface for working with multiple async types:
- **core.async** channels
- **CompletableFuture** (Java)
- **Future** (Java)
- **Manifold Deferred**
- **IDeref** types (promises, delays, atoms, refs)

All these types can be consumed and composed using a consistent API, making it easy to work with heterogeneous async code.

## Features

- **Unified Async Operations**: Use `!<!` and `!<!!` to read from any async type with automatic exception handling
- **Async/Thread Macros**: Create async operations that return any supported async type via configurable factories
- **Cancellation Support**: Cancel async operations across all supported types with `async-cancel!` and check status with `async-cancelled?`
- **Collection Utilities**: `async-map`, `async-reduce`, `async-for`, `async-some`, `async-every?`, and more
- **Thread Pool Management**: Route work to appropriate pools (`:io`, `:compute`, `:mixed`) for optimal resource usage
- **Deep Nesting Support**: Automatically unwrap nested async values
- **Exception-as-Value**: Uncaught exceptions are returned as values and rethrown on read
- **Thread Binding Preservation**: Dynamic `binding`s are preserved across parks and cross-thread resumes, in both `async` and `go` blocks (see [Load Order](#load-order))

## Quick Start

```clj
(ns user
  (:require [futurama.core :refer [async thread !<!! !<!]])
  (:import [java.util.concurrent CompletableFuture]))

;; Read from any async type
(defn fetch-data []
  (CompletableFuture/completedFuture {:status "ok"}))

(!<!! (fetch-data))
;;=> {:status "ok"}

;; Use inside async blocks
(async
  (let [result (!<! (fetch-data))]
    (println "Got:" result)))

;; Compose different async types
(async
  (let [cf-result (!<! (CompletableFuture/completedFuture 42))
        chan-result (!<! (async/go 10))
        fut-result (!<! (future (+ 1 2)))]
    (+ cf-result chan-result fut-result)))
;;=> returns async result with value 55
```

## Usage Examples

### Basic Async Operations

```clj
(require '[futurama.core :refer [async thread async-for !<!! !<!]])
(require '[clojure.core.async :refer [timeout]])

;; Create an async operation (parks, doesn't block threads)
(def result
  (async
    (!<! (timeout 100))
    "completed"))

;; Block waiting for result
(!<!! result)
;;=> "completed"

;; Create a thread operation (runs on thread pool)
(def heavy-work
  (thread
    (Thread/sleep 100)
    (* 42 42)))

(!<!! heavy-work)
;;=> 1764
```

### Async For Comprehensions

```clj
;; Sequential evaluation (items processed one by one)
(!<!!
  (async-for
    [a (range 4)
     b (range 4)
     :let [c (+ a b)]
     :when (and (odd? a) (odd? b))]
    (!<! (timeout 50))
    [a b c (+ a b c)]))
;;=> [[1 1 2 4] [1 3 4 8] [3 1 4 8] [3 3 6 12]]
;;   Takes ~200ms (4 items × 50ms each)

;; Concurrent evaluation (items processed in parallel)
(!<!!
  (async-for
    [a (range 4)
     b (range 4)
     :let [c (+ a b)]
     :when (and (odd? a) (odd? b))]
    (async
      (!<! (timeout 50))
      [a b c (+ a b c)])))
;;=> [[1 1 2 4] [1 3 4 8] [3 1 4 8] [3 3 6 12]]
;;   Takes ~50ms (all items processed concurrently)
```

### Cancellation

Futurama supports cancellation of async operations across all supported types. When an operation is cancelled, it receives a `CancellationException`.

```clj
(require '[futurama.core :refer [async async-cancel! async-cancelled?
                                 async-cancellable? thread]])

;; Cancel a long-running operation
(def work
  (async
    (loop [i 0]
      (when-not (async-cancelled?)
        (!<! (timeout 100))
        (println "Working..." i)
        (recur (inc i))))))

;; Check if it can be cancelled
(async-cancellable? work)
;;=> true

;; Cancel it
(async-cancel! work)

;; Check if it was cancelled
(async-cancelled? work)
;;=> true
```

#### Cooperative Cancellation

For best results, long-running operations should cooperatively check for cancellation:

```clj
(defn cancellable-work [items]
  (async
    (loop [remaining items
           results []]
      (if (or (empty? remaining) (async-cancelled?))
        results
        (let [item (first remaining)
              result (!<! (process-item item))]
          (recur (rest remaining) (conj results result)))))))
```

#### Parent-Child Cancellation

Child async operations automatically detect when their parent is cancelled:

```clj
(def parent
  (async
    (let [child1 (async
                   (loop []
                     (when-not (async-cancelled?)  ; Detects parent cancellation
                       (!<! (timeout 100))
                       (recur))))
          child2 (thread
                   (while (not (async-cancelled?))  ; Also detects parent cancellation
                     (Thread/sleep 100)))]
      (!<! child1)
      (!<! child2))))

;; Cancelling parent will cause children to detect cancellation
(async-cancel! parent)
```

### Working with Different Async Types

```clj
(require '[manifold.deferred :as d])
(require '[clojure.core.async :as a])

;; Mix and match async types freely
(async
  (let [from-future (!<! (future (+ 1 2)))
        from-deferred (!<! (d/success-deferred 10))
        from-channel (!<! (a/go 20))
        from-completable (!<! (CompletableFuture/completedFuture 30))]
    (+ from-future from-deferred from-channel from-completable)))
;;=> returns async result with value 63
```

### Thread Pool Management

Route work to appropriate thread pools for optimal performance:

```clj
;; Use :io pool for I/O-bound work (default for async)
(async :io
  (!<! (http-request "https://example.com")))

;; Use :compute pool for CPU-intensive work
(async :compute
  (compute-fibonacci 1000))

;; Use :mixed pool for mixed workloads (default for thread)
(thread :mixed
  (do-some-work))

;; Bind a custom pool temporarily
(require '[futurama.core :refer [with-pool]])
(require '[java.util.concurrent Executors])

(def my-pool (Executors/newFixedThreadPool 4))

(with-pool my-pool
  (async
    (println "Running on custom pool")))
```

### Collection Utilities

```clj
(require '[futurama.core :refer [async-map async-reduce async-some async-every?]])

;; async-map: Apply async function to collections
(!<!!
  (async-map
    (fn [x] (async (!<! (timeout 50)) (* x 2)))
    (range 5)))
;;=> [0 2 4 6 8]

;; async-reduce: Reduce with async operations
(!<!!
  (async-reduce
    (fn [acc x] (async (+ acc x)))
    0
    (range 10)))
;;=> 45

;; async-some: Find first truthy result
(!<!!
  (async-some
    (fn [x]
      (async
        (!<! (timeout 50))
        (when (> x 5) x)))
    (range 10)))
;;=> 6

;; async-every?: Check if all satisfy predicate
(!<!!
  (async-every?
    (fn [x]
      (async
        (!<! (timeout 50))
        (< x 10)))
    (range 5)))
;;=> true
```

### Configuring Async Factories

Control what type of async result is returned:

```clj
(require '[futurama.core :refer [set-async-factory! set-thread-factory!
                                 async-future-factory async-channel-factory
                                 async-promise-factory async-deferred-factory]])

;; Use CompletableFuture for async operations
(set-async-factory! async-future-factory)

(type (async "hello"))
;;=> java.util.concurrent.CompletableFuture

;; Use Manifold Deferred
(set-async-factory! async-deferred-factory)

(type (async "hello"))
;;=> manifold.deferred.Deferred

;; Reset to default (core.async promise-chan)
(set-async-factory! nil)
```

See the [tests](test/futurama/core_test.clj) for more examples.

## Installation

Add Futurama to your project dependencies:

[![Clojars Project](https://clojars.org/com.github.gateless/futurama/latest-version.svg)](http://clojars.org/com.github.gateless/futurama)

**deps.edn**
```clojure
{:deps {com.github.gateless/futurama {:mvn/version "1.4.9"}}}
```

**Leiningen project.clj**
```clojure
[com.github.gateless/futurama "1.4.9"]
```

## Load Order

> [!IMPORTANT]
> **Load `futurama.core` before any namespace that contains `go` or `async` blocks.**

To preserve dynamic bindings across parks, futurama patches two internals of core.async when
`futurama.core` loads (see [Dynamic Bindings](#dynamic-bindings)). Both patches are consumed at
**macroexpansion time** — `go` and `async` bake the generated state machine into your compiled
code. A namespace compiled *before* futurama loads keeps the unpatched, race-prone machine
permanently, for the life of that JVM. Requiring futurama later does not fix it retroactively, and
nothing warns you: bindings just go missing occasionally, under load.

Make `futurama.core` the first require in your entrypoint namespace:

```clj
(ns myapp.main
  (:require
   ;; MUST be first — futurama patches core.async's state-machine codegen at load
   ;; time, and `go` bakes that codegen in at macroexpansion. Any namespace compiled
   ;; before this line keeps the unpatched machine and can lose bindings across parks.
   [futurama.core]
   [myapp.handlers]
   [myapp.system])
  (:gen-class))
```

`:require` specs load in the order listed, so being first in the vector is sufficient — provided
this namespace is the root of your load graph. Note this deliberately breaks alphabetical require
sorting; if your linter enforces it, suppress it on this form rather than reordering.

**AOT compilation.** The same rule applies at *build* time. If you AOT-compile (uberjar,
`compile`, GraalVM native-image), load futurama before compiling anything containing `go` blocks:

```clj
;; build.clj
(require 'futurama.core)   ; patch core.async before compiling
(b/compile-clj {:basis basis :ns-compile '[myapp.main] :class-dir class-dir})
```

**Verifying the patch is live.** The terminator table names futurama's functions once patched:

```clj
(require '[clojure.core.async.impl.ioc-macros :as rt])
(get rt/async-custom-terminators 'clojure.core.async/<!)
;;=> futurama.core-async-patching/ioc-take!
```

This confirms the patch is *installed*; it does not tell you whether an already-compiled namespace
picked it up. When in doubt, check that your entrypoint requires `futurama.core` first.

> **Edge case:** the patches are applied with `alter-var-root`. If core.async is AOT-compiled with
> `-Dclojure.compiler.direct-linking=true`, the call into `emit-state-machine` is inlined and the
> patch cannot take effect. core.async ships source-only, so this only arises if you AOT-compile
> core.async yourself with direct linking enabled.

## Requirements

- **Clojure**: 1.11+ (backwards compatible with 1.10+)
- **Java**: 11+
- **core.async**: 1.8+

## API Reference

### Core Operations

| Function | Description |
|----------|-------------|
| `!<!` | Read from async value inside `async` block, parks if needed, throws exceptions |
| `!<!!` | Read from async value, blocks thread until ready, throws exceptions |
| `!<!*` | Read from collection of async values inside `async` block |
| `<!` | Read from async value without throwing exceptions (returns exception as value) |
| `<!!` | Blocking read without throwing exceptions (returns exception as value) |
| `<!*` | Read from collection of async values without throwing exceptions |

### Async Creation

| Macro/Function | Description |
|----------------|-------------|
| `async` | Create async operation that parks instead of blocking (like `go`) |
| `thread` | Create async operation that runs on thread pool |
| `async?` | Check if value is an async type |
| `async-completed?` | Check if async operation has completed |

### Cancellation

| Function | Description |
|----------|-------------|
| `async-cancel!` | Cancel an async operation |
| `async-cancelled?` | Check if operation is cancelled (also checks parent operations) |
| `async-cancellable?` | Check if value can be cancelled |

### Collection Operations

| Function | Description |
|----------|-------------|
| `async-for` | Async comprehension like `for`, supports `!<!` in body |
| `async-map` | Map with async function over collections |
| `async-reduce` | Reduce with async operations |
| `async-some` | Find first truthy async result |
| `async-every?` | Check if all async results are truthy |
| `async-walk` | Walk data structure with async operations |
| `async-postwalk` | Post-order async walk |
| `async-prewalk` | Pre-order async walk |

### Threading Macros

| Macro | Description |
|-------|-------------|
| `async->` | Thread-first with async operations |
| `async->>` | Thread-last with async operations |

### Thread Pool Management

| Function/Macro | Description |
|----------------|-------------|
| `with-pool` | Execute body with specified thread pool |
| `get-pool` | Get thread pool for workload type (`:io`, `:compute`, `:mixed`) |

### Factory Configuration

| Function | Description |
|----------|-------------|
| `set-async-factory!` | Set global factory for `async` macro results |
| `set-thread-factory!` | Set global factory for `thread` macro results |
| `with-async-factory` | Temporarily bind async factory |
| `with-thread-factory` | Temporarily bind thread factory |
| `async-channel-factory` | Creates core.async channel (buffer size 1) |
| `async-promise-factory` | Creates core.async promise-chan (default) |
| `async-future-factory` | Creates CompletableFuture |
| `async-deferred-factory` | Creates Manifold Deferred |

## Advanced Configuration

### Custom Thread Pools

Configure application-wide thread pools using the Java system property `futurama.executor-factory`:

```clojure
;; Use core.async's thread pool
(System/setProperty "futurama.executor-factory"
                    "clojure.core.async.impl.dispatch/executor-for")
```

The factory function receives a keyword (`:io`, `:compute`, `:mixed`) and should return an `Executor` or `nil` to use defaults.

### Exception Handling

Futurama treats exceptions as values. Uncaught exceptions are:
1. Captured and returned over the async channel/future
2. Re-thrown when read via `!<!` or `!<!!`
3. Wrapped/unwrapped automatically (`ExecutionException`, `CompletionException`)

When operations are cancelled, they receive a `CancellationException`:

```clojure
(let [work (async
             (loop []
               (when-not (async-cancelled?)
                 (!<! (timeout 100))
                 (recur))))]
  (async-cancel! work)
  (try
    (!<!! work)
    (catch CancellationException e
      (println "Operation was cancelled"))))
```

### Dynamic Bindings

Dynamic var bindings established with `binding` are preserved across parking
operations — including when a parked `async` or `go` block resumes on a
different thread from the pool. Bindings set *inside* a block before a park are
retained after it resumes as well.

```clojure
(def ^:dynamic *request-id* nil)

(binding [*request-id* "abc-123"]
  (!<!! (async
          (!<! (timeout 100))   ; parks; may resume on another pool thread
          *request-id*)))
;;=> "abc-123"
```

> **Note:** futurama preserves bindings by patching two core.async internals when
> `futurama.core` loads: the parking terminators (`<!`, `>!`, `alts!`) are replaced with versions
> that snapshot the thread binding frame *before* registering the resume callback, and
> `clojure.core.async.impl.go/emit-state-machine` is replaced with a version that no longer writes
> the binding frame back in a `finally` (where a slower parking thread could clobber a frame
> already advanced by a resume on another thread). Both are global, JVM-wide effects that apply to
> every `go` block compiled after the library loads, and are a temporary measure pending an
> upstream core.async fix.
>
> Because they take effect at macroexpansion time, **load order matters** — see
> [Load Order](#load-order).

## Building

This project uses [Clojure CLI tools](https://clojure.org/guides/deps_and_cli) and GNU Make.

```bash
# Run tests
make test

# Run tests with core.async 1.9 (next version)
make test-next

# Start REPL
make repl

# Run linter
make lint

# Build JAR
make build
```

## Communication

- **Issues**: Report bugs or request features via [GitHub Issues](https://github.com/gateless/futurama/issues)
- **Discussions**: Ask questions or share ideas in [GitHub Discussions](https://github.com/gateless/futurama/discussions)

# Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

# YourKit

[![YourKit](https://www.yourkit.com/images/yklogo.png)](https://www.yourkit.com/)

YourKit supports open source projects with innovative and intelligent tools
for monitoring and profiling Java and .NET applications.
YourKit is the creator of [YourKit Java Profiler](https://www.yourkit.com/java/profiler/),
[YourKit .NET Profiler](https://www.yourkit.com/dotnet-profiler/),
and [YourKit YouMonitor](https://www.yourkit.com/youmonitor/).

# LICENSE

Copyright 2024 Gateless

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

&nbsp;&nbsp;&nbsp;&nbsp;http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
