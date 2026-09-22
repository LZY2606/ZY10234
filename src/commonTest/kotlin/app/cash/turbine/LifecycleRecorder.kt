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
import kotlin.test.assertEquals
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.test.TestScope

/**
 * Test-side [LifecycleSink].
 *
 * Events are stored in an unlimited channel so recording can never block the library under
 * observation, even from the undispatched collector or from a `invokeOnCompletion` handler. All
 * assertions operate on a drained snapshot and print the full timeline on failure.
 */
internal class LifecycleRecorder : LifecycleSink {
  private val events = Channel<LifecycleEvent>(UNLIMITED)

  override fun record(event: LifecycleEvent) {
    events.trySend(event)
  }

  /** Drains every event recorded so far. Never suspends and preserves recording order. */
  fun snapshot(): List<LifecycleEvent> = buildList {
    while (true) {
      add(events.tryReceive().getOrNull() ?: break)
    }
  }
}

/**
 * Installs this recorder in a coroutine context, e.g. `withContext(recorder.asElement()) { .. }`.
 */
internal fun LifecycleRecorder.asElement(): CoroutineContext = LifecycleSinkElement(this)

/**
 * Asserts that the recorded timeline equals [expected] and prints both timelines side by side on
 * failure so lifecycle ordering bugs are debuggable without a debugger.
 */
internal fun LifecycleRecorder.expectEvents(vararg expected: LifecycleEvent) {
  expectEvents(expected.toList())
}

internal fun LifecycleRecorder.expectEvents(expected: List<LifecycleEvent>) {
  val actual = snapshot()
  assertEquals(
    expected.renderTimeline(),
    actual.renderTimeline(),
    "Lifecycle timeline did not match",
  )
}

/**
 * Drains the recorder and returns the named timeline. Use with [containsTimeline] for prefix/order
 * checks where the tail is nondeterministic (e.g. [kotlinx.coroutines.test.runTest] scope
 * teardown).
 */
internal fun LifecycleRecorder.timeline(): List<String> = snapshot().map { it.name() }

internal fun LifecycleRecorder.containsTimeline(vararg expected: String) {
  val actual = timeline()
  val actualJoined = actual.renderNames()
  val expectedJoined = expected.toList().renderNames()
  assertEquals(
    true,
    actual.windowed(expected.size).any { it == expected.toList() },
    "Expected timeline to contain:\n$expectedJoined\n\nActual timeline:\n$actualJoined",
  )
}

internal fun LifecycleRecorder.singleEvent(): LifecycleEvent =
  snapshot().also { check(it.size == 1) { "Expected one event, got:\n${it.renderTimeline()}" } }[0]

/**
 * Asserts that [expected] occurs as an ordered subsequence of the recorded timeline. Use this
 * instead of [expectEvents] when some transitions are scheduled concurrently (e.g. collector job
 * completion interleaving with scope completion handlers).
 */
internal fun LifecycleRecorder.assertSubsequence(vararg expected: String) {
  val actual = timeline()
  var index = 0
  for (event in actual) {
    if (index < expected.size && event == expected[index]) index++
  }
  if (index != expected.size) {
    throw AssertionError(
      "Expected timeline to contain in order:\n${expected.joinToString("\n") { " - $it" }}\n\n" +
        "Actual timeline:\n${actual.joinToString("\n") { " - $it" }}"
    )
  }
}

private fun List<LifecycleEvent>.renderTimeline(): String = map { it.name() }.renderNames()

private fun List<String>.renderNames(): String =
  if (isEmpty()) "(empty)" else joinToString("\n") { " - $it" }

private fun LifecycleEvent.name(): String =
  when (this) {
    is LifecycleEvent.RegistryScopeEntered -> "RegistryScopeEntered(nested=$nested)"
    is LifecycleEvent.CollectorStarted -> "CollectorStarted($owner)"
    is LifecycleEvent.ChannelSent -> "ChannelSent($event)"
    is LifecycleEvent.ChannelClosed -> "ChannelClosed($event)"
    is LifecycleEvent.JobCompleted -> "JobCompleted(cancelled=$cancelled, cause=$cause)"
    is LifecycleEvent.ItemAdded -> "ItemAdded($event)"
    is LifecycleEvent.CloseRequested -> "CloseRequested($cause)"
    is LifecycleEvent.CancelRequested -> "CancelRequested(source=$source, openForSend=$openForSend)"
    is LifecycleEvent.EventsDrained -> "EventsDrained(${events.joinToString(", ")})"
    is LifecycleEvent.AwaitEntered -> "AwaitEntered"
    is LifecycleEvent.AwaitExited -> "AwaitExited($outcome)"
    is LifecycleEvent.TimeoutResolved ->
      "TimeoutResolved(timeout=$timeout, source=$source, mechanism=$mechanism)"
    is LifecycleEvent.UnconsumedChecked ->
      "UnconsumedChecked(origin=$origin, nonEmpty=$nonEmpty, events=[${events.joinToString(", ")}])"
    is LifecycleEvent.ScopeExiting -> "ScopeExiting($exception)"
    is LifecycleEvent.ScopeFailure -> "ScopeFailure($exception)"
  }

/** A standalone [Turbine] wired to this recorder. */
internal fun <T> LifecycleRecorder.turbine(
  timeout: kotlin.time.Duration? = null,
  name: String? = null,
): Turbine<T> = recordingTurbine(this, timeout, name)

/** Runs [block] as a `runTest` body with the recorder installed and virtual time. */
internal fun runRecordedTest(block: suspend TestScope.(LifecycleRecorder) -> Unit) =
  kotlinx.coroutines.test.runTest {
    val recorder = LifecycleRecorder()
    kotlinx.coroutines.withContext(recorder.asElement()) { block(recorder) }
  }
