/*
 * Copyright (C) 2026 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package app.cash.turbine

import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration

/**
 * Where the effective timeout of an `await*` call came from.
 *
 * Resolution order (highest priority first):
 * 1. [Explicit] &mdash; the `timeout` argument of `Turbine()`, `testIn`, or `test` (which installs
 *    it as a context override inside its block).
 * 2. [Context] &mdash; a [TurbineTimeoutElement] installed by `withTurbineTimeout`.
 * 3. [Default] &mdash; the library default when neither of the above is present.
 */
internal enum class TimeoutSource {
  Explicit,
  Context,
  Default,
}

/**
 * Whether an `await*` timeout is enforced with virtual time or a wall-clock delay.
 *
 * Inside a `runTest` scheduler the virtual clock is controlled by the test and must not be allowed
 * to advance while Turbine waits, so a wall-clock timer on a separate scope is used. Everywhere
 * else the regular `withTimeout` (virtual-time aware) is used.
 */
internal enum class TimeoutMechanism {
  VirtualTime,
  WallClock,
}

/**
 * Internal observer for the lifecycle shared by [Flow.test], [testIn], standalone [Turbine]s and
 * the `ReceiveChannel` extensions.
 *
 * Every method has an empty default so instrumentation can opt into the events it cares about.
 * Hooks are best-effort structural notifications: they must not throw or alter behavior, and they
 * are not part of the public API.
 */
internal interface TurbineLifecycleListener {
  /** The coroutine collecting a flow into a turbine has started. */
  fun collectorStarted() {}

  /** The flow collection job completed (normally, with an error, or because it was cancelled). */
  fun collectJobCompleted(cause: Throwable?) {}

  /** A value was sent into the turbine's channel. */
  fun channelSent(item: Any?) {}

  /** A value or terminal event was received from the turbine's channel. */
  fun channelReceived(event: Event<*>) {}

  /** The turbine's channel was closed. [cause] is non-null for a failed close. */
  fun channelClosed(cause: Throwable?) {}

  /** The turbine's channel was cancelled. */
  fun channelCancelRequested(cause: Throwable?) {}

  /** A cancel request reached the turbine (its channel and any backing collect job). */
  fun cancelRequested() {}

  /** An `await*` call began suspending for an event. */
  fun awaitEntered(source: TimeoutSource, timeout: Duration, mechanism: TimeoutMechanism) {}

  /** An `await*` call returned (with an event) or threw (including a timeout/assertion failure). */
  fun awaitExited(event: Event<*>?, failure: Throwable?) {}

  /** A timeout duration was resolved for an `await*` call. */
  fun timeoutResolved(source: TimeoutSource, timeout: Duration, mechanism: TimeoutMechanism) {}

  /**
   * An unconsumed-event check ran. [unconsumed] is the report that would be surfaced; [failure] is
   * non-null when the check surfaced it as an assertion error.
   */
  fun unconsumedChecked(report: UnconsumedEventReport<*>, failure: AssertionError?) {}
}

/** Carries the active [TurbineLifecycleListener]s through coroutine context. */
internal class TurbineLifecycleElement(val listeners: List<TurbineLifecycleListener>) :
  CoroutineContext.Element {
  companion object Key : CoroutineContext.Key<TurbineLifecycleElement>

  override val key: CoroutineContext.Key<*> = Key
}

/**
 * Present while [ChannelTurbine] owns the await lifecycle for a call. Suppresses the per-channel
 * notifications so that each `await*` produces exactly one enter/exit pair.
 */
internal object TurbineLifecycleForwardingKey : CoroutineContext.Key<TurbineLifecycleForwarding>

/**
 * Installed by [ChannelTurbine] while an `await*` runs. [awaitEvent] notifies exactly these
 * listeners (which include the turbine's own listeners and any context listeners), producing one
 * enter/exit pair that carries the resolved event instead of a duplicate, suppressed pair.
 */
internal class TurbineLifecycleForwarding(val listeners: List<TurbineLifecycleListener>) :
  CoroutineContext.Element {
  override val key: CoroutineContext.Key<*> = TurbineLifecycleForwardingKey
}
