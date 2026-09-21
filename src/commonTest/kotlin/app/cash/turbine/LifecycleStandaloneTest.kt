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
 * Lifecycle anatomy for standalone [Turbine] instances (manual `add`/`close`), and the four
 * distinct states a flow-backed turbine can occupy around cancellation.
 */
class LifecycleStandaloneTest {
  @Test
  fun standaloneTurbineHasNoCollectorAndRecordsManualSendReceive() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      val turbine = recordedTurbine<String>(recorder)
      turbine.add("a")
      turbine.add("b")
      assertEquals("a", turbine.awaitItem())
      assertEquals("b", turbine.awaitItem())
      turbine.close()
      turbine.awaitComplete()
      turbine.ensureAllEventsConsumed()
    }

    // A standalone turbine never starts a collector or collect job.
    recorder.assertNotContainsTrace(LifecycleTrace.CollectorStarted)
    recorder.assertContainsTrace(LifecycleTrace.ChannelSent("a"))
    recorder.assertContainsTrace(LifecycleTrace.ChannelSent("b"))
    recorder.assertContainsTrace(LifecycleTrace.ChannelClosed(null))
    val check = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().single()
    assertEquals(emptyList(), check.unconsumed)
  }

  @Test
  fun addAfterCloseFailsAndDoesNotEmitSentTrace() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      val turbine = recordedTurbine<String>(recorder)
      turbine.add("first")
      turbine.close()
      assertFailsWith<IllegalStateException> { turbine.add("second") }
      turbine.cancelAndIgnoreRemainingEvents()
    }

    val sent = recorder.tracesOfType<LifecycleTrace.ChannelSent>().map { it.value }
    assertEquals(listOf("first"), sent)
  }

  /**
   * The four states the task calls out, produced in one deterministic timeline: A. upstream
   * completion event enqueued but not yet consumed; B. collecting job completed; C. ReceiveTurbine
   * canceled; D. test scope finished.
   */
  @Test
  fun fourStatesAreDistinguishable() = runTest {
    val recorder = LifecycleRecorder()
    val itemGate = CompletableDeferred<Unit>()

    // A gated flow lets us park the collector after its first emission, then complete on demand.
    val source = flow {
      emit("only")
      itemGate.await()
    }

    withLifecycleRecorder(recorder) {
      turbineScope {
        val turbine = source.testIn(this)

        // Drain the first item so the collector is suspended at the gate (state B is not yet
        // reached: the job is active).
        assertEquals("only", turbine.awaitItem())
        itemGate.complete(Unit)
        // Give the undispatched completion a chance to run without advancing any clock.
        kotlinx.coroutines.yield()

        // State A: Complete has been enqueued; not consumed yet. State B: collect job completed.
        recorder.assertContainsTrace(LifecycleTrace.ChannelClosed(null))
        recorder.assertContainsTrace(LifecycleTrace.CollectJobCompleted(null))

        // Consume completion, then explicitly cancel -> state C.
        turbine.awaitComplete()
        turbine.cancel()
        recorder.assertContainsTrace(LifecycleTrace.CancelRequested)
      }
    }

    // State D: scope finished. The unconsumed check for this turbine ran at least once and found
    // nothing remaining.
    val checks = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>()
    assertTrue(checks.isNotEmpty(), recorder.describe())
    assertTrue(checks.all { it.unconsumed.isEmpty() }, recorder.describe())
  }

  @Test
  fun enqueuedCompleteWithJobCompletedIsReportedWhenNeverObserved() = runTest {
    val recorder = LifecycleRecorder()

    val failure =
      assertFailsWith<TurbineAssertionError> {
        withLifecycleRecorder(recorder) {
          // Completion is enqueued and the collect job completes before the block ends.
          kotlinx.coroutines.flow.flowOf("x").test {
            // Consume the item but leave the enqueued completion untouched.
            assertEquals("x", awaitItem())
          }
        }
      }
    assertEquals("Unconsumed events found:\n - Complete", failure.message)
    val check =
      recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().first { it.unconsumed.isNotEmpty() }
    assertEquals(listOf(Event.Complete), check.unconsumed)
  }

  @Test
  fun cancelledBeforeTerminalEventIgnoresTerminalEvent() = runTest {
    val recorder = LifecycleRecorder()
    val gate = CompletableDeferred<Unit>()

    withLifecycleRecorder(recorder) {
      flow {
        emit("kept")
        gate.await()
      }
        .test {
          assertEquals("kept", awaitItem())
          cancel() // canceled while upstream still suspended: no terminal event buffered
          gate.complete(Unit)
        }
    }

    // cancel() on an open channel sets ignoreTerminalEvents, so no unconsumed failure surfaces.
    val check = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().last()
    assertEquals(emptyList(), check.unconsumed, recorder.describe())
  }

  @Test
  fun cancelAndIgnoreSwallowsBufferedComplete() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      kotlinx.coroutines.flow.flowOf("x").test {
        assertEquals("x", awaitItem())
        // Complete already buffered (state A). Ignore it instead of consuming.
        cancelAndIgnoreRemainingEvents()
      }
    }

    val check = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().last()
    assertEquals(emptyList(), check.unconsumed, recorder.describe())
  }

  @Test
  fun cancelAndIgnoreSwallowsBufferedError() = runTest {
    val recorder = LifecycleRecorder()
    val error = CustomThrowable("ignored")

    withLifecycleRecorder(recorder) {
      flow {
        emit("x")
        throw error
      }
        .test {
          assertEquals("x", awaitItem())
          // Error already buffered (state A). Ignore it; the cause is not rethrown.
          cancelAndIgnoreRemainingEvents()
        }
    }

    val check = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().last()
    assertEquals(emptyList(), check.unconsumed, recorder.describe())
  }

  @Test
  fun cancelAndConsumeReturnsBufferedComplete() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      kotlinx.coroutines.flow.flowOf("x").test {
        assertEquals("x", awaitItem())
        val remaining = cancelAndConsumeRemainingEvents()
        assertEquals(listOf<Event<*>>(Event.Complete), remaining)
      }
    }

    val check = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().last()
    assertEquals(emptyList(), check.unconsumed, recorder.describe())
  }

  @Test
  fun cancelAndConsumeReturnsBufferedErrorAndDoesNotThrowIt() = runTest {
    val recorder = LifecycleRecorder()
    val error = CustomThrowable("returned")

    withLifecycleRecorder(recorder) {
      flow {
        emit("x")
        throw error
      }
        .test {
          assertEquals("x", awaitItem())
          // The buffered error is drained and returned, not thrown from the consuming cancel.
          val remaining = cancelAndConsumeRemainingEvents()
          assertEquals(listOf<Event<*>>(Event.Error(error)), remaining)
          assertSame(error, (remaining.single() as Event.Error).throwable)
        }
    }

    val check = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().last()
    assertEquals(emptyList(), check.unconsumed, recorder.describe())
  }

  @Test
  fun awaitingBufferedErrorStillThrowsItToCaller() = runTest {
    val recorder = LifecycleRecorder()
    val error = CustomThrowable("awaited")

    withLifecycleRecorder(recorder) {
      flow {
        emit("x")
        throw error
      }
        .test {
          assertEquals("x", awaitItem())
          val observed = awaitError()
          assertSame(error, observed)
        }
    }
  }
}
