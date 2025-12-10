This is a history of changes to gateless/futurama

# 1.4.1
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
