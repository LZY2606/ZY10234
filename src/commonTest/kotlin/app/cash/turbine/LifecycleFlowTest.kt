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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.test.runTest

/**
 * Deterministic lifecycle anatomy for `Flow.test`.
 *
 * Every scenario uses `runTest` virtual time and [CompletableDeferred] barriers; there are no
 * real-clock waits.
 */
class LifecycleFlowTest {
  @Test
  fun testStartsCollectorBeforeBlockAndCancelsOnNormalCompletion() = runTest {
    val recorder = LifecycleRecorder()
    val collectorCanProceed = CompletableDeferred<Unit>()

    withLifecycleRecorder(recorder) {
      flow {
        emit("one")
        collectorCanProceed.await()
        emit("two")
      }
        .test {
          // Collector is already running (UNDISPATCHED launch) and the first item is buffered.
          recorder.assertPrefix(
            listOf(
              LifecycleTrace.CollectorStarted,
              LifecycleTrace.ChannelSent("one"),
            )
          )

          assertEquals("one", awaitItem())
          collectorCanProceed.complete(Unit)
          assertEquals("two", awaitItem())
          awaitComplete()
        }
    }

    val traces = recorder.traces
    // Block finished -> test cancels (collector already completed normally) then verifies.
    assertTrue(LifecycleTrace.CollectorStarted in traces)
    assertEquals(
      LifecycleTrace.ChannelClosed(null),
      traces.last { it is LifecycleTrace.ChannelClosed },
    )
    assertTrue(traces.any { it is LifecycleTrace.UnconsumedChecked })
    val check = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().single()
    assertEquals(emptyList(), check.unconsumed)
  }

  @Test
  fun testBlockThrowsObservesBlockFailureAndStillRunsUnconsumedCheck() = runTest {
    val recorder = LifecycleRecorder()
    val blockFailure = CustomThrowable("block failure")

    val actual =
      assertFailsWith<CustomThrowable> {
        withLifecycleRecorder(recorder) {
          flow {
            emit("item")
            awaitGate()
          }
            .test {
              assertEquals("item", awaitItem())
              throw blockFailure
            }
        }
      }
    assertSame(blockFailure, actual)

    // An upstream error arriving through the cancelled hierarchy is only aggregated if it carries
    // a non-cancellation cause; here the check still ran once and did not replace the block error.
    val checks = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>()
    assertEquals(1, checks.size, recorder.describe())
  }

  @Test
  fun upstreamErrorIsAnEventObservedByAwaitAndCauseIsAttached() = runTest {
    val recorder = LifecycleRecorder()
    val upstream = CustomThrowable("boom")

    withLifecycleRecorder(recorder) {
      flow<Nothing> { throw upstream }
        .test {
          val observed = awaitError()
          assertSame(upstream, observed)
        }
    }

    recorder.assertContainsTrace(LifecycleTrace.ChannelClosed("CustomThrowable"))
    val received = recorder.tracesOfType<LifecycleTrace.ChannelReceived>()
    assertTrue(
      received.any { (it.event as? Event.Error)?.throwable === upstream },
      recorder.describe(),
    )
    val exit = recorder.tracesOfType<LifecycleTrace.AwaitExit>().last()
    assertSame(upstream, (exit.event as Event.Error).throwable)
  }

  @Test
  fun bufferedUpstreamErrorAfterBlockExitBecomesUnconsumedCause() = runTest {
    val recorder = LifecycleRecorder()
    val upstream = CustomThrowable("buffered")

    val failure =
      assertFailsWith<TurbineAssertionError> {
        withLifecycleRecorder(recorder) {
          flow {
            emit("item")
            throw upstream
          }
            .test {
              assertEquals("item", awaitItem())
              // Error is now buffered but not consumed when the block completes normally.
            }
        }
      }
    assertSame(upstream, failure.cause)
    // The terminal error is drained by the first report; later catch-all aggregation sees an empty
    // report, so assert against the first check that carried it.
    val check =
      recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().first { it.unconsumed.isNotEmpty() }
    assertEquals(listOf(Event.Error(upstream)), check.unconsumed)
    assertEquals("TurbineAssertionError", check.failureDescription)
  }
}

/** A gate that is never completed within the test, keeping a flow suspended. */
private suspend fun awaitGate(): Unit = kotlinx.coroutines.awaitCancellation()
