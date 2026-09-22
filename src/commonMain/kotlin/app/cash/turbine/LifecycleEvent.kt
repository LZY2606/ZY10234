/*
 * Copyright (C) 2024 Square, Inc.
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

/**
 * An internal, side-effect free observation of a single lifecycle transition.
 *
 * The events are shared by every Turbine entry point (`test`, `testIn`, standalone [Turbine], and
 * the `ReceiveChannel` extensions) so that the ownership and cancellation differences between them
 * can be observed deterministically from tests. Recording an event must never change runtime
 * behavior: recorders are optional and failures inside a recorder must not reach the library.
 */
internal sealed interface LifecycleEvent {
  /**
   * A `turbineScope` block was entered. [nested] is true when an outer scope already owns the
   * registry (the outer scope wins and no new registry element is installed).
   */
  public data class RegistryScopeEntered(public val nested: Boolean) : LifecycleEvent

  /** Collection of an upstream flow started. [owner] is `test` or `testIn`. */
  public data class CollectorStarted(public val owner: String) : LifecycleEvent

  /** The collector sent an item into the unlimited buffer. */
  public data class ChannelSent(public val event: String) : LifecycleEvent

  /** The collector closed the buffer, either normally or with a throwable. */
  public data class ChannelClosed(public val event: String) : LifecycleEvent

  /**
   * The collection job completed. [cancelled] reports the final job state and [cause] is null on
   * normal completion and a class name on cancellation or failure.
   */
  public data class JobCompleted(public val cancelled: Boolean, public val cause: String?) :
    LifecycleEvent

  /** An item was added manually through [Turbine.add]. */
  public data class ItemAdded(public val event: String) : LifecycleEvent

  /** The turbine was closed manually through [Turbine.close]. */
  public data class CloseRequested(public val cause: String?) : LifecycleEvent

  /**
   * A cancel was requested. [source] identifies the API (`cancel`, `cancelAndIgnoreRemainingEvents`
   * or `cancelAndConsumeRemainingEvents`) and [openForSend] reports whether the buffer was still
   * open, i.e. whether the upstream was still alive at that instant.
   */
  public data class CancelRequested(public val source: String, public val openForSend: Boolean) :
    LifecycleEvent

  /** `cancelAndConsumeRemainingEvents` drained the buffered events, terminal event included. */
  public data class EventsDrained(public val events: List<String>) : LifecycleEvent

  /** An `await*` call started waiting on its channel. */
  public data object AwaitEntered : LifecycleEvent

  /** An `await*` call stopped waiting; [outcome] is an event name, `Timeout` or `Cancelled`. */
  public data class AwaitExited(public val outcome: String) : LifecycleEvent

  /**
   * The effective timeout for an await was resolved.
   *
   * [source] records which level supplied it: `Explicit` (parameter to `test`, `testIn` or
   * `Turbine`, including the element the `test` parameter installs), `Context`
   * (`withTurbineTimeout`) or `Default` (3 seconds). [mechanism] is `WallClock` when a
   * `TestCoroutineScheduler` is present and `Virtual` otherwise.
   */
  public data class TimeoutResolved(
    public val timeout: String,
    public val source: String,
    public val mechanism: String,
  ) : LifecycleEvent

  /**
   * An unconsumed-event report ran. [origin] is `ensureAllEventsConsumed`, `scopeCompletion` (a
   * `testIn` scope ending) or `turbineScope` (a validation block threw). [nonEmpty] reports whether
   * the report found (or, for a scope failure, aggregated) anything.
   */
  public data class UnconsumedChecked(
    public val origin: String,
    public val events: List<String>,
    public val nonEmpty: Boolean,
  ) : LifecycleEvent

  /**
   * The scope a `testIn` turbine was launched in is completing, with [exception] null on normal
   * completion and a class name otherwise.
   */
  public data class ScopeExiting(public val exception: String?) : LifecycleEvent

  /** A validation block threw; the report has already been aggregated from all turbines. */
  public data class ScopeFailure(public val exception: String) : LifecycleEvent
}

/** Receives [LifecycleEvent]s. Intentionally minimal so test recorders cannot feed events back. */
internal fun interface LifecycleSink {
  public fun record(event: LifecycleEvent)
}

internal class LifecycleSinkElement(public val sink: LifecycleSink) : CoroutineContext.Element {
  public companion object Key : CoroutineContext.Key<LifecycleSinkElement>

  override val key: CoroutineContext.Key<*> = Key
}

internal val CoroutineContext.lifecycleSink: LifecycleSink?
  get() = this[LifecycleSinkElement.Key]?.sink

internal fun Throwable?.eventName(): String? = this?.let { it::class.simpleName ?: "Throwable" }
