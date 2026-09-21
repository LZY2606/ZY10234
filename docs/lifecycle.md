# Turbine Lifecycle Anatomy

This document is a reference for the lifecycle shared by the four ways events enter Turbine:

| Entry point | Starts collection? | Owns a cancel? | Runs `ensureAllEventsConsumed`? |
| --- | --- | --- | --- |
| `Flow.test { }` | Yes, before the block runs | Yes — when the block returns/throws | Yes — after cancel |
| `Flow.testIn(scope)` | Yes, before it returns | No — caller or terminal event | Yes — when `scope` completes |
| `Turbine()` (standalone) | No | No job; `cancel()` closes the channel | Only on explicit call |
| `ReceiveChannel` extensions | No | No | No |

All four speak the same [`Event`](../src/commonMain/kotlin/app/cash/turbine/Event.kt) model:
`Item`, `Complete`, `Error`. The behavioral assertions in this document are pinned by deterministic
tests under `src/commonTest/.../Lifecycle*Test.kt` (JVM-only timing details live in
`src/jvmTest/.../LifecycleJvmTest.kt`). The tests use `runTest` virtual time and
`CompletableDeferred` barriers only — no real-clock waits, no network, no filesystem ordering.

## Observation hooks

`src/commonMain/.../lifecycle.kt` defines an internal `TurbineLifecycleListener` with one hook per
structural transition. It is not public API and carries no behavior:

- `collectorStarted`, `collectJobCompleted(cause)`
- `channelSent(value)`, `channelReceived(event)`, `channelClosed(cause)`,
  `channelCancelRequested(cause)`
- `cancelRequested`
- `awaitEntered(source, timeout, mechanism)` / `awaitExited(event, failure)`
- `timeoutResolved(source, timeout, mechanism)`
- `unconsumedChecked(report, failure)`

The test recorder (`LifecycleRecorder` in `LifecycleTrace.kt`) captures these in arrival order and
prints the full sequence on any mismatch, so failures are self-explaining.

## `Flow.test`

Implementation: [`flow.kt`](../src/commonMain/kotlin/app/cash/turbine/flow.kt) (`test`,
`collectTurbineIn`), [`Turbine.kt`](../src/commonMain/kotlin/app/cash/turbine/Turbine.kt).

`test` calls `turbineScope`, then launches collection **undispatched**
(`scope.launch(unconfined, start = UNDISPATCHED)`). Because the launch is undispatched, the flow
runs up to its first suspension point before the validation block starts — this is why collecting a
`SharedFlow` via `test` does not miss an immediately emitted value.

```
caller                collectTurbineIn            collect job            Channel(UNLIMITED)
  | test { } ------------>| launch(UNDISPATCHED)       |                          |
  |                        |--- collectorStarted ----->| collect { trySend }       |
  |                        |                           |--- sent/closed --------->|
  |<-- block receiver -----|                           |                          |
  | awaitItem() ----------> receiveCatching -----------|------------------------->|
  |<-- Item ----------------                           |                          |
  | block returns          |                           |                          |
  |--- cancel() ---------->| channel.cancel +         |                          |
  |                        | collectJob.cancelAndJoin  |                          |
  |--- ensureAllEventsConsumed ------------------------|------------------------->|
```

- **Who cancels:** `test` always calls `cancel()` after the block (normal return *or* throw), then
  `ensureAllEventsConsumed()`.
- **Who observes a block exception:** the block exception propagates unchanged out of `test`.
  Separately, `turbineScope`'s catch asks every registered turbine for an unconsumed-event report;
  a non-cancellation upstream error from a *different* turbine is wrapped in a
  `TurbineAssertionError` whose **cause is the original block exception** and whose message embeds
  the upstream stack trace. Same-turbine failures are surfaced by the direct
  `ensureAllEventsConsumed` call.
- **Who observes an upstream exception:** it is converted to `Event.Error` in the buffered channel
  and only becomes a thrown exception when consumed (`awaitError`) or left unconsumed (attached as
  the assertion's `cause`).

## `Flow.testIn`

`testIn` requires a `TurbineContext` (`turbineScope`); without the registry context element it
throws before launching anything. It returns immediately after the undispatched launch.

- **Foreground scope** (a child of the test body): the turbine is live until you `cancel()` it or
  consume a terminal event. An `invokeOnCompletion` handler calls `ensureAllEventsConsumed()` when
  the scope finishes.
- **Background scope** (`runTest`'s `backgroundScope`): the same handler fires during `runTest`
  teardown, *after* the test body. On JVM a handler failure surfaces as a
  `kotlinx.coroutines.CompletionHandlerException` wrapping the `AssertionError`; on non-JVM targets
  the cross-platform tests assert via an explicit cancel + `CoroutineExceptionHandler` instead of
  relying on post-body teardown timing.
- The handler skips the assertion when the scope completes with a non-cancellation exception; it
  runs on normal completion **and** cancellation (`CancellationException` counts as normal).

## Standalone `Turbine()`

A standalone turbine wraps an `UNLIMITED` channel with no collect job:

- `add` is a non-blocking `trySend`; `close(cause)` closes the channel.
- There is no collector start, no collect-job completion, and no automatic cancel.
- `cancel()` / `cancelAndIgnoreRemainingEvents()` / `cancelAndConsumeRemainingEvents()` are the
  caller's responsibility; `ensureAllEventsConsumed()` only runs when called explicitly.

## `ReceiveChannel` extensions

`awaitEvent`/`awaitItem`/`awaitComplete`/`awaitError` (in
[`channel.kt`](../src/commonMain/kotlin/app/cash/turbine/channel.kt)) operate on a plain channel:
they produce only an await enter/exit pair, never start a collector and never validate
consumption. They are the layer every other entry point funnels through for waiting and timeout.

## Four distinct states

"Canceled" and "done" are not one state. Around a flow-backed turbine these are independently
observable (see `LifecycleStandaloneTest.fourStatesAreDistinguishable`):

1. **Terminal event enqueued, not consumed** — the collector closed the `UNLIMITED` channel; an
   `Event.Complete`/`Event.Error` is buffered. The collector may already be finished while the
   test still holds an unconsumed event.
2. **Collect job completed** — the `Job` launched by `collectTurbineIn` is no longer active,
   observed through `collectJobCompleted`. A buffered terminal event and a completed job normally
   coincide, but `ChannelReceived`/drain state is separate from `Job` state.
3. **`ReceiveTurbine` canceled** — `cancel()` ran: `cancelRequested`, the delegating channel's
   `cancel(cause)` (`channelCancelRequested`), and `collectJob.cancelAndJoin()`.
4. **Test scope finished** — the scope completion handler (testIn) or `test` tail ran the
   unconsumed-event check.

One subtlety: *observing* a terminal event through a receive sets an internal
`ignoreRemainingEvents` flag (the channel delegations in `Turbine.kt`), so a later unconsumed
check treats that terminal event as handled. A terminal event that was never observed remains
reportable.

## Ignoring vs. consuming cancellation

Both methods cancel the turbine; they differ in what happens to an already buffered
`Complete`/`Error` (pinned by `LifecycleStandaloneTest`):

| Method | Buffered items | Buffered `Complete`/`Error` | Throws the buffered error? | Returned |
| --- | --- | --- | --- | --- |
| `cancel()` then scope exit | reported if unconsumed | ignored only if channel was still open at cancel time | via unconsumed assertion `cause` | — |
| `cancelAndIgnoreRemainingEvents()` | ignored | ignored | No | — |
| `cancelAndConsumeRemainingEvents()` | drained | returned as the final element | No — returned inside `Event.Error` | `List<Event<T>>` |

Concretely, `cancelAndConsumeRemainingEvents` drains via `takeEventUnsafe()` **before** canceling,
stopping at the first terminal event; `cancelAndIgnoreRemainingEvents` cancels first and sets
`ignoreRemainingEvents`. `awaitError()` against a buffered error still throws it to the caller —
returning vs. throwing depends on the API, not on whether the error was buffered.

## Timeout sources and precedence

Resolution lives in [`coroutines.kt`](../src/commonMain/kotlin/app/cash/turbine/coroutines.kt)
(`resolveTimeout`) and is consumed in [`channel.kt`](../src/commonMain/kotlin/app/cash/turbine/channel.kt).

Precedence, highest first:

1. **Explicit** — the `timeout` argument of `Turbine(timeout=…)`, `testIn(timeout=…)`, or
   `test(timeout=…)`/`turbineScope(timeout=…)`. `test` installs its argument as an override element
   for the whole block, so it beats an enclosing `withTurbineTimeout`.
2. **Context** — a `TurbineTimeoutElement` installed by `withTurbineTimeout`.
3. **Default** — 3 seconds when neither is present.

Enforcement mechanism is decided from the coroutine context:

- With a `TestCoroutineScheduler` in context (`runTest`), `withTimeout` would consume the virtual
  clock and hang, so Turbine uses a **wall-clock** timer: the awaited block runs undispatched and a
  timeout coroutine is launched on `GlobalScope` + `Dispatchers.Default`, racing via `select`.
  Turbine timeouts therefore ignore the test's virtual clock.
- Without a test scheduler, the regular virtual-time-aware `withTimeout` is used.

The lifecycle tests assert the resolved `(source, duration, mechanism)` without ever firing a
timeout (an item is always already available), so they perform no real delay. Firing behavior
remains covered by the pre-existing `failsOnDefaultTimeout` and related tests.

## Complexity

- Each `await*` adds one notification pass over a small listener list (typically 0–1) and one
  `withContext` element install; waiting complexity is unchanged (`O(1)` bookkeeping; the suspend
  cost is dominated by the channel receive).
- `reportUnconsumedEvents`/`ensureAllEventsConsumed` drain buffered events once — `O(n)` in the
  number of buffered events, as before. Instrumentation adds an `O(L)` listener fan-out where `L`
  is the number of installed (test-only) listeners.
- The recorder stores one entry per structural event; tests that emit many events pay linear
  memory.

## Compatibility notes

- **Public API is unchanged.** No new public declarations were added; the binary/signature dumps
  under `api/` are unaffected. Hooks, elements, and the recorder are `internal` (the recorder lives
  in the test source set).
- **Behavior is unchanged.** The only production-path additions are best-effort listener
  invocations around existing transitions and an explicit `source` carried internally by the
  timeout context element; timeouts still resolve to the same durations and throw the same
  `TurbineAssertionError` messages/causes.
- **Platforms are unchanged.** The common test suite passes on JVM, JS (Node), Wasm/JS, and native
  (`macosArm64` in CI here). Tests avoid JVM-only constructs except the dedicated `jvmTest` class
  (stack-frame shape, `Throwable.suppressed`, and post-`runTest` teardown exception timing).
