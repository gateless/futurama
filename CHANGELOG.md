This is a history of changes to gateless/futurama

# 1.4.9
* **`finally` binding race**: Fixed a race where bindings established *inside* an `async`/`go` block could still be lost across a park, even with 1.4.8's snapshotting terminators. core.async's generated state machine writes the current thread binding frame back into the shared state array in a `finally` on every exit from a run. When a park resumes on another pool thread before the parking thread unwinds, that `finally` overwrites `BINDINGS-IDX` with a stale frame, discarding whatever the resumed run established. futurama now patches `clojure.core.async.impl.go/emit-state-machine` to omit the `finally` write entirely; the binding frame is captured only by the parking terminators, before the resume callback is registered. Both halves are required — 1.4.8 shipped only the terminators.
* **New namespace**: `futurama.core-async-patching` (`^:no-doc`) now owns every core.async monkey-patch — the `emit-state-machine` replacement and the `ioc-take!`/`ioc-put!`/`ioc-alts!` terminators, relocated from `futurama.impl`. One namespace, one responsibility: everything futurama alters in core.async is visible in a single file.
* **Load order requirement**: Both patches are applied at **macroexpansion time**. Namespaces containing `go` or `async` blocks that are compiled before `futurama.core` loads keep the unpatched state machine, silently and permanently for that JVM. Load `futurama.core` as one of the first namespaces at application start — see [Load Order](README.md#load-order). This also applies at *build* time for AOT/uberjar/native-image compilation.
* **Tests**: Added `binding-nested-inside-async-and-go-block-survives-park`, covering nested `binding` forms with parks interspersed in both `async` and plain `go` blocks (500 iterations each, park window deterministically widened). Retargeted the existing binding-race tests at `futurama.core-async-patching/ioc-take!`.

# 1.4.8
* **Thread binding preservation**: Dynamic var bindings established with `binding` are now preserved across parking operations (`<!`, `>!`, `alts!` and the futurama `!<!` equivalents), even when a parked `async`/`go` block resumes on a different pool thread. Previously, a resume that fired on another thread before the parking thread finished saving its binding frame could observe a stale frame and lose bindings set inside the block. This is fixed by custom IOC "parking terminators" (`futurama.impl/ioc-take!`, `ioc-put!`, `ioc-alts!`) that snapshot the current thread binding frame *before* registering the resume callback. Validated against core.async 1.8 and 1.9.
* **core.async `go` coverage**: `async!` uses the snapshotting terminators directly. Because plain core.async `go` blocks read their terminators from a hardcoded var at macroexpansion time (no injection point), futurama also alters the root of `clojure.core.async.impl.ioc-macros/async-custom-terminators` so `go` blocks compiled after the library loads get the same guarantee. This is a global, JVM-wide effect and a temporary measure pending an upstream core.async fix.
* **AsyncReader read path**: Consolidated the `ReadPort/take!` logic into `futurama.impl/async-read-port-take!`, adding a `poll!` synchronous fast-path for ready core.async channels and committing the read handler up front. The up-front commit fixes a bug where reading a plain (non-async) value through an `->async-reader` inside `alts!` could leave a phantom taker on the losing port that later consumed and dropped a value.
* **Tests**: Added binding-survival coverage (bindings set inside and outside `async`/`go`, verified over thousands of iterations with the park window deterministically widened) and `->async-reader`/`alts!` read-port coverage. Replaced the `bond` `get-pool` spy — which was subject to cross-thread pollution — with pure, deterministic macroexpansion assertions, and replaced criterium benchmarks with a lightweight `min-elapsed-ms` helper.
* **Tooling**: Switched the format aliases to `dev.weavejester/cljfmt 0.16.5` (from `cljfmt/cljfmt 0.9.2`), consolidated indentation config into a single `.cljfmt.edn` (unqualified symbol keys, so clojure-lsp and the CLI stay aligned), and removed the now-unused `circleci/bond` test dependency. Added an `antq` dependency-report tool (`:deps-antq` alias) with `make deps-check` / `make deps-upgrade` targets that exclude `org.clojure/core.async` (which is version-matrixed by hand), and bumped the CI actions (`actions/checkout`, `jdx/mise-action`).
* **Dependencies**: Upgraded long-stale dev/test/build deps to latest: `clj-kondo` (2026.07.24), `test.check` (1.1.3), `slf4j-simple` (2.0.18), `nrepl` (1.7.0), `cider-nrepl` (0.62.2), `graal-build-time` (1.0.6), and `deps-deploy` (0.2.5). Dropped `pjstadig/humane-test-output` (no longer recommended for use) along with its activation in the test setup.

# 1.4.7
* **Membership predicates**: Centralized the scattered `satisfies?` protocol checks behind inlining predicate macros in `futurama.impl` (`async?`, `async-channel?`, `async-completable-reader?`, `async-completable-writer?`, `async-cancellable?`), and switched `futurama.core`/`futurama.impl` call sites over to them. The public `futurama.core/async?` remains a function so it can still be passed as a higher-order value.
* **Fast path**: `!<!` and `!<!!` now short-circuit non-async values, returning them directly without a channel round-trip; the argument expression is still evaluated exactly once.
* **Tests**: Added coverage for the `!<!`/`!<!!` non-async fast path, including the single-evaluation guarantee for both non-async and async argument expressions.
* **Dependencies**: Updated Clojure to 1.12.5 and Caffeine to 3.2.4.

# 1.4.6
* **Pool contract**: `get-pool` once again returns an `ExecutorService` (1.4.5 had narrowed the return type to `Executor` to track core.async 1.9's `executor-for`, which broke downstream consumers that called `.submit`/`.invokeAll` on the pool). When `executor-for` returns a plain `Executor`, the result is widened via a new `futurama.impl/->executor-service` proxy that forwards `execute`; real `ExecutorService` instances pass through unwrapped. The internal `async-dispatch-task-handler` continues to accept any `Executor`.
* **Proxy lifecycle**: The widening proxy throws `UnsupportedOperationException` from `shutdown`, `shutdownNow`, `isShutdown`, `isTerminated`, and `awaitTermination` — it does not own the underlying executor and refuses to lie about its lifecycle. Callers that need to manage a pool's lifecycle should hold a reference to the original `Executor` directly.
* **Tests**: Added coverage for `->executor-service` (passthrough vs. wrap, `execute`/`submit` routing, lifecycle behavior on both branches) and for the previously-untested `with-async-factory` and `with-thread-factory` macros (binding/restore, precedence, nesting, exception unwind).

# 1.4.5
* **Dispatch**: `async-dispatch-task-handler` now accepts any `java.util.concurrent.Executor` (previously required `ExecutorService`). Tasks are wrapped in a `FutureTask` so cancel-with-interrupt and exception-capture semantics are preserved. Required to support core.async 1.9's `clojure.core.async.impl.dispatch/executor-for`, which returns a plain `Executor`.
* **Default pool**: `get-pool` now falls back to `clojure.core.async.impl.dispatch/executor-for` (was `ForkJoinPool/commonPool`). Out of the box, futurama dispatches over the same workload-aware pools as core.async — no `futurama.executor-factory` sysprop required for that behavior. The sysprop remains the override hook.
* **Tooling**: Bumped CI and `.mise.toml` JDK to corretto-25 (latest LTS); updated `.mise.toml` to the new `[tool_alias]` schema; default `TEST_CORE_ASYNC_ALIAS` flipped to `core.async-1.9` in the Makefile.

# 1.4.3
* **Dependencies**: Updated core.async to stable 1.9.865 (default), Clojure to 1.12.4, manifold to 0.5.0, Caffeine to 3.2.3.
* **Tooling**: Updated clj-kondo to 2026.04.15 and kaocha to 1.91.1392.

# 1.4.2
* **Feature**: Add utility `->future` function to easily convert values to future.

# 1.4.1
* **Bug Fix**: Ensure future is only cancelled if async item was cancelled.

# 1.4.0
* **Enhanced Cancellation**: Improved cancellation support now works across all async types including core.async channels
* **State Management**: Replaced WeakHashMap with Caffeine cache for lock-free, thread-safe cancellation state tracking
* **Dependencies**: Added Caffeine 3.2.2, updated Clojure to 1.12.3, core.async to 1.8.741 (with 1.9.829-alpha2 support)
* **CI Updates**: Test matrix now covers Clojure 1.12 with core.async 1.8/1.9 on Java 11 & 24 (library remains backwards compatible)
* **Improvements**: Channel factories use identity exception handler for robust error handling, consistent protocol return values

# 1.3.1
* update core async version to latest `1.8.735`

# 1.3.0
* remove deprecated `completable-future` and `fixed-threadpool`
* update core async version to latest `1.8.730`

# 1.2.0
* remove async-{future/channel/promise/...} variants of the `async` macro, replace uses with `async`.
* added `thread`, updated `async`, both macros route work to the appropriate thread pool, such as :io, :compute, or :mixed.
* added `*thread-factory*` dynamic binding to allow separately defining a factory-fn for `thread` calls, distinct from `async` calls.
* deprecate the `completable-future` macro, uses of completable-future should be replaced with `thread`.
* deprecate the `fixed-threadpool` function, instead prefer to use Executors thread pools according to need.
* updated library documentation, better document available async/thread factories and provide a smaller more stable footprint.

# 1.1.0
* replace default async channel factory used in async macro with async promise-channel factory, for more consistent with future/promise behavior.
* add support for core.async > 1.7.x with backwards compabitility for core.async 1.6.x and lower
* add tests matrix for:
  - java: 11, 21
  - clojure: 1.10, 1.11, 1.12
  - core.async: 1.6, 1.7, 1.8

# 1.0.5
* replace uses of instance-satisfies? with clojure.core/satisfies?.

# 1.0.4
* replace uses of Reify with JavaFunction and JavaBiConsumer types
* upgrade clojure version to 1.11.4

# 1.0.3
* upgrade clojure version to 1.11.2

# 1.0.2
* minimize use of weak references, only pushing one to the global state when an async item is cancelled
* synchronize global state for cancellations using a reentrant readwrite lock instead of default lock

# 1.0.1
* enhance reader and writer impl to better support nested async values
* shorten class names for async reader and rethrow fns used inside macro

# 1.0.0
* separate ReadPort and WritePort impl into its own namespace
* initial major release of library with updated protocols impl and same API

# 0.6.7
* fix not calling realized? when not IPending

# 0.6.6
* fix arity problem with async-reduce reducer

# 0.6.5
* change default thread pool to ForkJoinPool/commonPool

# 0.6.4
* Create <! <!! and <!* version of take macros which do not recursive read.
* Only !<!, !<!! and !<!* explicitly recursive read from channels now, to optimize things.
* Simplified ReadPort implementations so they do not recursive read, only specific macros do that now.

# 0.6.3
* Simplify reading macros !<! and !<!! using new AsyncReader type
* Refactor async reader functions into util reusable reading fn
* Add `async-cancellable?` fn to easily test if something can be cancelled

# 0.6.2
* Change default async output to channel of size 1 to more easily support async merge and other ops
* Add purpose-built `async-future` and `async-deferred` macros to more easily create either.

# 0.6.1
* Code linting updates

# 0.6.0
* Add some no-doc tags to extra namespaces
* Rename `cancel!` and `cancelled?` to `async-cancel!` and `async-cancelled?`

# 0.5.0
* Add custom state to keep track of async items
* Add custom cancel strategy which combines bound state, global weak state, and custom protocol impl

# 0.4.0
* Add `fixed-threadpool` method to create a FixedThreadPool
* The default `*thread-pool*` is now a FixedThreadPool which can be interrupted.
* Allow `async` to be interrupted just like `completable-future`, add tests.

# 0.3.9
* Add `with-pool` macro

# 0.3.8
* Add support for `Future` and `IDeref`.
* Rename and refactor internal `satisfies?` to `instance-satisfies?` and `class-satisfies?`

# 0.3.7
* Add collection helpers for: `async-reduce`, `async-some`, `async-every?`, `async-walk/prewalk/postwalk`

# 0.3.6
* Refactored `async-for` so it uses less async macros and it is more flexible
* Refactored `async-map` so it leverages `async-for` behind the scenes.
* Removed `async-some` and `async-every?` and instead added some new helpers.
* Added `async->` and `async->>` threading macros to make it easier to thread async.
* Replaced matching on Exception to Throwable to avoid leaving hanging promises due to errors.

# 0.3.5
* Add more async collection fns: `async-map`, `async-some`, `async-every?`

# 0.3.4
* Simplify `async-for` by just executing each iteration inside an async block and then collect after

# 0.3.3
* Add `async-for` comprehension which implicitly runs inside an async block

# 0.3.2
* Ensure Deferred is handled correctly on put! when realized

# 0.3.1
* Add new `async?` helper function which is useful

# 0.3.0
* Add better identity-like raw value handling for `!<!` and `!<!!`

# 0.2.2
* Add SCM to POM file

# 0.2.1
* Update exception handling to better deal with ExecutionException and CompletionException

# 0.2.0
* Add support for Manifold Deferred

# 0.1.0
* The initial release.
