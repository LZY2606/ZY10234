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

import kotlin.time.Duration
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext

/**
 * A single structural lifecycle notification emitted by the internal instrumentation hooks.
 *
 * These mirror, one-to-one, the hooks exercised by the lifecycle anatomy tests in
 * `LifecycleTest.kt`. They are test-only and intentionally data-like so failures print the whole
 * recorded sequence for context.
 */
internal sealed interface LifecycleTrace {
  data object CollectorStarted : LifecycleTrace

  data class CollectJobCompleted(val causeDescription: String?) : LifecycleTrace

  data class ChannelSent(val value: Any?) : LifecycleTrace

  data class ChannelReceived(val event: Event<*>) : LifecycleTrace

  data class ChannelClosed(val causeDescription: String?) : LifecycleTrace

  data class ChannelCancelRequested(val causeDescription: String?) : LifecycleTrace

  data object CancelRequested : LifecycleTrace

  data class AwaitEnter(
    val source: TimeoutSource,
    val timeout: Duration,
    val mechanism: TimeoutMechanism,
  ) : LifecycleTrace

  data class AwaitExit(val event: Event<*>?, val failureDescription: String?) : LifecycleTrace

  data class TimeoutResolved(
    val source: TimeoutSource,
    val timeout: Duration,
    val mechanism: TimeoutMechanism,
  ) : LifecycleTrace

  data class UnconsumedChecked(
    val unconsumed: List<Event<*>>,
    val failureDescription: String?,
  ) : LifecycleTrace
}

/**
 * Records every internal lifecycle notification in arrival order. All tests that exercise the
 * shared Event model install one of these so lifecycle ownership can be asserted deterministically.
 */
internal class LifecycleRecorder : TurbineLifecycleListener {
  private val mutableTraces = mutableListOf<LifecycleTrace>()

  val traces: List<LifecycleTrace>
    get() = mutableTraces.toList()

  fun reset() {
    mutableTraces.clear()
  }

  // Hooks are invoked from coroutines driven by a single test scheduler; the lifecycle tests never
  // allow the wall-clock timeout job to fire concurrently, so no platform locking is required.
  private fun record(trace: LifecycleTrace) {
    mutableTraces += trace
  }

  override fun collectorStarted() = record(LifecycleTrace.CollectorStarted)

  override fun collectJobCompleted(cause: Throwable?) =
    record(LifecycleTrace.CollectJobCompleted(cause?.let { it::class.simpleName ?: "Throwable" }))

  override fun channelSent(item: Any?) = record(LifecycleTrace.ChannelSent(item))

  override fun channelReceived(event: Event<*>) = record(LifecycleTrace.ChannelReceived(event))

  override fun channelClosed(cause: Throwable?) =
    record(LifecycleTrace.ChannelClosed(cause?.let { it::class.simpleName ?: "Throwable" }))

  override fun channelCancelRequested(cause: Throwable?) =
    record(
      LifecycleTrace.ChannelCancelRequested(cause?.let { it::class.simpleName ?: "Throwable" })
    )

  override fun cancelRequested() = record(LifecycleTrace.CancelRequested)

  override fun awaitEntered(
    source: TimeoutSource,
    timeout: Duration,
    mechanism: TimeoutMechanism,
  ) = record(LifecycleTrace.AwaitEnter(source, timeout, mechanism))

  override fun awaitExited(event: Event<*>?, failure: Throwable?) =
    record(
      LifecycleTrace.AwaitExit(
        event,
        failure?.let { it::class.simpleName ?: "Throwable" },
      )
    )

  override fun timeoutResolved(
    source: TimeoutSource,
    timeout: Duration,
    mechanism: TimeoutMechanism,
  ) = record(LifecycleTrace.TimeoutResolved(source, timeout, mechanism))

  override fun unconsumedChecked(
    report: UnconsumedEventReport<*>,
    failure: AssertionError?,
  ) =
    record(
      LifecycleTrace.UnconsumedChecked(
        unconsumed = report.unconsumed,
        failureDescription = failure?.let { it::class.simpleName ?: "AssertionError" },
      )
    )
}

/** Run [block] with [recorder] installed, adding to any enclosing lifecycle listeners. */
internal suspend fun <T> withLifecycleRecorder(
  recorder: LifecycleRecorder,
  block: suspend () -> T,
): T {
  val enclosing = currentCoroutineContext()[TurbineLifecycleElement]?.listeners.orEmpty()
  return withContext(TurbineLifecycleElement(enclosing + recorder)) { block() }
}

/**
 * Assert that the recorded trace starts with exactly [expected], printing the full recording on
 * failure so lifecycle ordering bugs are diagnosable without re-running with logging.
 */
internal fun LifecycleRecorder.assertPrefix(expected: List<LifecycleTrace>) {
  val actual = traces
  if (actual.size < expected.size || actual.subList(0, expected.size) != expected) {
    throw AssertionError(
      buildString {
        appendLine("Lifecycle prefix mismatch.")
        appendLine("Expected prefix:")
        expected.forEach { appendLine("  $it") }
        appendLine("Actual traces:")
        actual.forEach { appendLine("  $it") }
      }
    )
  }
}

internal fun LifecycleRecorder.assertContainsTrace(expected: LifecycleTrace) {
  val actual = traces
  if (expected !in actual) {
    throw AssertionError(
      buildString {
        appendLine("Expected trace not recorded: $expected")
        appendLine("Actual traces:")
        actual.forEach { appendLine("  $it") }
      }
    )
  }
}

internal fun LifecycleRecorder.assertNotContainsTrace(unexpected: LifecycleTrace) {
  val actual = traces
  if (unexpected in actual) {
    throw AssertionError(
      buildString {
        appendLine("Unexpected trace recorded: $unexpected")
        appendLine("Actual traces:")
        actual.forEach { appendLine("  $it") }
      }
    )
  }
}

internal inline fun <reified T : LifecycleTrace> LifecycleRecorder.tracesOfType(): List<T> =
  traces.filterIsInstance<T>()

internal fun LifecycleRecorder.describe(): String =
  traces.joinToString(separator = "\n") { "  $it" }

/**
 * Construct a standalone [Turbine] with [recorder] attached as a structural lifecycle observer.
 *
 * The public [Turbine] factory cannot read coroutine context (it is not a `suspend` function), so
 * tests install the observer through the internal [ChannelTurbine] constructor. Public behavior is
 * identical to `Turbine(timeout, name)`.
 */
internal fun <T> recordedTurbine(
  recorder: LifecycleRecorder,
  timeout: Duration? = null,
  name: String? = null,
): Turbine<T> =
  ChannelTurbine(
    channel = Channel(UNLIMITED),
    collectJob = null,
    timeout = timeout,
    name = name,
    extraLifecycleListeners = listOf(recorder),
  )
