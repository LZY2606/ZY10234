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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.CompletionHandlerException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext

/**
 * Deterministic lifecycle anatomy of every Turbine entry point.
 *
 * All tests run under [runTest] (virtual time) and synchronize with the collector's undispatched
 * start or rendezvous barriers rather than real delays. Failures render the full recorded timeline,
 * see [LifecycleRecorder].
 */
class LifecycleTest {
  // -------------------------------------------------------------------------------------------
  // Flow.test
  // -------------------------------------------------------------------------------------------

  @Test
  fun testStartsCollectorUndispatchedThenCancelsAndVerifiesOnNormalCompletion() =
    runRecordedTest { recorder ->
      flowOf("one").test {
        assertEquals("one", awaitItem())
        awaitComplete()
      }

      recorder.expectEvents(
        LifecycleEvent.RegistryScopeEntered(nested = false),
        LifecycleEvent.CollectorStarted("test"),
        LifecycleEvent.ChannelSent("Item(one)"),
        LifecycleEvent.ChannelClosed("Complete"),
        LifecycleEvent.JobCompleted(cancelled = false, cause = null),
        LifecycleEvent.TimeoutResolved("3s", "Default", "WallClock"),
        LifecycleEvent.AwaitEntered,
        LifecycleEvent.AwaitExited("Item(one)"),
        LifecycleEvent.TimeoutResolved("3s", "Default", "WallClock"),
        LifecycleEvent.AwaitEntered,
        LifecycleEvent.AwaitExited("Complete"),
        LifecycleEvent.CancelRequested("cancel", openForSend = false),
        LifecycleEvent.UnconsumedChecked("ensureAllEventsConsumed", emptyList(), false),
      )
    }

  @Test
  fun testCollectorRunsToFirstSuspensionBeforeValidationBlock() = runRecordedTest { recorder ->
    val started = Job()
    val release = Job()
    flow<Nothing> {
        started.complete()
        release.join()
      }
      .test {
        assertTrue(started.isCompleted, "collector must have reached its first suspension point")
        release.complete()
        awaitComplete()
      }

    recorder.assertSubsequence(
      "RegistryScopeEntered(nested=false)",
      "CollectorStarted(test)",
      "ChannelClosed(Complete)",
    )
  }

  @Test
  fun testBlockThrowingIsRethrownDirectlyAndCancelsCollector() = runRecordedTest { recorder ->
    val expected = CustomThrowable("boom")

    val actual = assertFailsWith<CustomThrowable> { neverFlow().test { throw expected } }
    assertSame(expected, actual)

    recorder.assertSubsequence(
      "CollectorStarted(test)",
      "ScopeFailure(CustomThrowable)",
      "UnconsumedChecked(origin=turbineScope, nonEmpty=false, events=[])",
      "ChannelClosed(Error(JobCancellationException))",
      "JobCompleted(cancelled=true, cause=JobCancellationException)",
    )
  }

  @Test
  fun upstreamErrorProducesEventAndIsObservedByAwaitErrorNotThrownFromCollect() =
    runRecordedTest { recorder ->
      val expected = CustomThrowable("upstream")
      flow<Nothing> { throw expected }
        .test {
          assertSame(expected, awaitError())
        }

      recorder.expectEvents(
        LifecycleEvent.RegistryScopeEntered(nested = false),
        LifecycleEvent.CollectorStarted("test"),
        LifecycleEvent.ChannelClosed("Error(CustomThrowable)"),
        LifecycleEvent.JobCompleted(cancelled = false, cause = null),
        LifecycleEvent.TimeoutResolved("3s", "Default", "WallClock"),
        LifecycleEvent.AwaitEntered,
        LifecycleEvent.AwaitExited("Error(CustomThrowable)"),
        LifecycleEvent.CancelRequested("cancel", openForSend = false),
        LifecycleEvent.UnconsumedChecked("ensureAllEventsConsumed", emptyList(), false),
      )
    }

  @Test
  fun unconsumedUpstreamErrorIsReportedAsAssertionErrorWithCause() = runRecordedTest { recorder ->
    val expected = RuntimeException("upstream")

    val actual = assertFailsWith<AssertionError> { flow<Nothing> { throw expected }.test {} }

    assertEquals(
      """
      |Unconsumed events found:
      | - Error(RuntimeException)
      """
        .trimMargin(),
      actual.message,
    )
    assertSame(expected, actual.cause)

    recorder.assertSubsequence(
      "CollectorStarted(test)",
      "ChannelClosed(Error(RuntimeException))",
      "UnconsumedChecked(origin=ensureAllEventsConsumed, nonEmpty=true, events=[Error(RuntimeException)])",
    )
  }

  @Test
  fun cancelJoinsCollectorBeforeReturning() = runRecordedTest { recorder ->
    var collecting = false
    neverFlow()
      .onStart { collecting = true }
      .onCompletion {
        withContext(NonCancellable) {
          collecting = false
        }
      }
      .test {
        cancel()
        assertEquals(false, collecting, "cancel() joins the collector before returning")
      }

    // User cancel while the channel is still open; the cancelled collector closes it with the
    // CancellationException, and test still performs its own end-of-block cancel (a no-op join).
    recorder.assertSubsequence(
      "CancelRequested(source=cancel, openForSend=true)",
      "ChannelClosed(Error(JobCancellationException))",
      "JobCompleted(cancelled=true, cause=JobCancellationException)",
      "CancelRequested(source=cancel, openForSend=false)",
      "UnconsumedChecked(origin=ensureAllEventsConsumed, nonEmpty=false, events=[])",
    )
  }

  // -------------------------------------------------------------------------------------------
  // Flow.testIn: foreground scope owns the turbine; the scope drives the final check.
  // -------------------------------------------------------------------------------------------

  @Test
  fun testInForegroundScopeCollectsUntilAwaitCompleteAndChecksOnScopeExit() =
    runRecordedTest { recorder ->
      turbineScope {
        coroutineScope {
          val turbine = flowOf("one").testIn(this)
          assertEquals("one", turbine.awaitItem())
          turbine.awaitComplete()
        }
      }

      recorder.expectEvents(
        LifecycleEvent.RegistryScopeEntered(nested = false),
        LifecycleEvent.CollectorStarted("testIn"),
        LifecycleEvent.ChannelSent("Item(one)"),
        LifecycleEvent.ChannelClosed("Complete"),
        LifecycleEvent.JobCompleted(cancelled = false, cause = null),
        LifecycleEvent.TimeoutResolved("3s", "Default", "WallClock"),
        LifecycleEvent.AwaitEntered,
        LifecycleEvent.AwaitExited("Item(one)"),
        LifecycleEvent.TimeoutResolved("3s", "Default", "WallClock"),
        LifecycleEvent.AwaitEntered,
        LifecycleEvent.AwaitExited("Complete"),
        LifecycleEvent.ScopeExiting(null),
        LifecycleEvent.UnconsumedChecked("scopeCompletion", emptyList(), false),
      )
    }

  @Test
  fun testInForegroundScopeUnconsumedItemFailsWhenScopeIsCancelled() = runRecordedTest { recorder ->
    val handler = RecordingExceptionHandler()
    val job =
      launch(handler, start = CoroutineStart.UNDISPATCHED) {
        withContext(recorder.asElement()) {
          turbineScope {
            flow {
                emit("item!")
                emitAll(neverFlow())
              }
              .testIn(this)
          }
        }
      }
    job.cancel()

    val reported = handler.exceptions.single()
    assertTrue(reported is CompletionHandlerException, "got $reported")
    val cause = reported.cause
    assertTrue(cause is AssertionError, "expected handler to receive the assertion, got $cause")
    assertEquals(
      """
      |Unconsumed events found:
      | - Item(item!)
      """
        .trimMargin(),
      cause.message,
    )

    recorder.assertSubsequence(
      "CollectorStarted(testIn)",
      "ChannelSent(Item(item!))",
      "ScopeExiting(JobCancellationException)",
      "UnconsumedChecked(origin=scopeCompletion, nonEmpty=true, events=[Item(item!)])",
    )
  }

  // -------------------------------------------------------------------------------------------
  // Flow.testIn: runTest's backgroundScope owns the turbine and outlives the turbineScope block.
  // -------------------------------------------------------------------------------------------

  @Test
  fun testInBackgroundScopeIsStillAliveWhenTurbineScopeExits() = runRecordedTest { recorder ->
    turbineScope {
      val turbine = neverFlow().testIn(backgroundScope)
      turbine.expectNoEvents()
    }

    // The backgroundScope is cancelled by runTest teardown, outside the recorder-installed
    // context; the collector remains active for the whole validation block.
    recorder.expectEvents(
      LifecycleEvent.RegistryScopeEntered(nested = false),
      LifecycleEvent.CollectorStarted("testIn"),
    )
  }

  @Test
  fun testInBackgroundScopeExplicitCancelJoinsCollector() = runRecordedTest { recorder ->
    turbineScope {
      val turbine = neverFlow().testIn(backgroundScope)
      turbine.cancel()
    }

    recorder.expectEvents(
      LifecycleEvent.RegistryScopeEntered(nested = false),
      LifecycleEvent.CollectorStarted("testIn"),
      LifecycleEvent.CancelRequested("cancel", openForSend = true),
      LifecycleEvent.ChannelClosed("Error(JobCancellationException)"),
      LifecycleEvent.JobCompleted(cancelled = true, cause = "JobCancellationException"),
    )
  }

  // -------------------------------------------------------------------------------------------
  // Standalone Turbine: no collector job exists; the user owns every transition.
  // -------------------------------------------------------------------------------------------

  @Test
  fun standaloneTurbineIsDrivenEntirelyByManualCalls() = runRecordedTest { recorder ->
    val turbine = recorder.turbine<String>()
    turbine.add("a")
    turbine.close()
    assertEquals("a", turbine.awaitItem())
    turbine.awaitComplete()
    turbine.ensureAllEventsConsumed()

    recorder.expectEvents(
      LifecycleEvent.ItemAdded("Item(a)"),
      LifecycleEvent.CloseRequested(null),
      LifecycleEvent.TimeoutResolved("3s", "Default", "WallClock"),
      LifecycleEvent.AwaitEntered,
      LifecycleEvent.AwaitExited("Item(a)"),
      LifecycleEvent.TimeoutResolved("3s", "Default", "WallClock"),
      LifecycleEvent.AwaitEntered,
      LifecycleEvent.AwaitExited("Complete"),
      LifecycleEvent.UnconsumedChecked("ensureAllEventsConsumed", emptyList(), false),
    )
  }

  @Test
  fun standaloneTurbineCancelHasNoCollectorJobToJoin() = runRecordedTest { recorder ->
    val turbine = recorder.turbine<String>()
    turbine.add("a")
    turbine.cancelAndIgnoreRemainingEvents()
    turbine.ensureAllEventsConsumed()

    recorder.expectEvents(
      LifecycleEvent.ItemAdded("Item(a)"),
      LifecycleEvent.CancelRequested("cancelAndIgnoreRemainingEvents", openForSend = true),
      LifecycleEvent.UnconsumedChecked("ensureAllEventsConsumed", emptyList(), false),
    )
  }

  // -------------------------------------------------------------------------------------------
  // The four distinct terminal states:
  //  1. terminal event enqueued but not yet consumed
  //  2. collection job completed
  //  3. ReceiveTurbine cancelled while upstream is still running
  //  4. owning scope finished
  // -------------------------------------------------------------------------------------------

  @Test
  fun stateOneTerminalEventEnqueuedButNotConsumed() = runRecordedTest { recorder ->
    val error =
      assertFailsWith<AssertionError> {
        flowOf("item!").test {
          assertEquals("item!", awaitItem())
          // Collector has closed the channel and the Complete event sits in the unlimited buffer.
        }
      }
    assertEquals(
      """
      |Unconsumed events found:
      | - Complete
      """
        .trimMargin(),
      error.message,
    )

    recorder.assertSubsequence(
      "ChannelSent(Item(item!))",
      "ChannelClosed(Complete)",
      "AwaitExited(Item(item!))",
      "UnconsumedChecked(origin=ensureAllEventsConsumed, nonEmpty=true, events=[Complete])",
    )
  }

  @Test
  fun stateTwoCollectorJobCompletedIsObservableAndLaterCancelIsANoOpJoin() =
    runRecordedTest { recorder ->
      flowOf("one").test {
        assertEquals("one", awaitItem())
        awaitComplete()
        // The collection job is already completed; the unconditional end-of-block cancel() then
        // performs a cancelAndJoin on a completed job and leaves the (await-consumed, and therefore
        // ignored) terminal event alone.
      }

      val lifecycle =
        recorder.timeline().filter {
          it.startsWith("JobCompleted") ||
            it.startsWith("CancelRequested") ||
            it.startsWith("UnconsumedChecked")
        }
      assertEquals(
        listOf(
          "JobCompleted(cancelled=false, cause=null)",
          "CancelRequested(source=cancel, openForSend=false)",
          "UnconsumedChecked(origin=ensureAllEventsConsumed, nonEmpty=false, events=[])",
        ),
        lifecycle,
      )
    }

  @Test
  fun stateThreeCancelledTurbineDoesNotReportTerminalEvents() = runRecordedTest { recorder ->
    flow {
      emit("one")
      emitAll(neverFlow())
    }
      .test {
        assertEquals("one", awaitItem())
        cancel()
      }

    recorder.assertSubsequence(
      "ChannelSent(Item(one))",
      "CancelRequested(source=cancel, openForSend=true)",
      "ChannelClosed(Error(JobCancellationException))",
      "JobCompleted(cancelled=true, cause=JobCancellationException)",
      "UnconsumedChecked(origin=ensureAllEventsConsumed, nonEmpty=false, events=[])",
    )
  }

  @Test
  fun stateFourScopeFinishedChecksTestInTurbines() = runRecordedTest { recorder ->
    // The scope completion handler throws; runTest defers it, so capture it with a handler.
    val handler = RecordingExceptionHandler()
    withContext(handler) {
      turbineScope {
        emptyFlow<Nothing>().testIn(this)
      }
    }
    val thrown = handler.exceptions.single()
    assertTrue(thrown is CompletionHandlerException, "got $thrown")
    assertTrue(thrown.cause is AssertionError, "got ${thrown.cause}")

    recorder.assertSubsequence(
      "CollectorStarted(testIn)",
      "ChannelClosed(Complete)",
      "JobCompleted(cancelled=false, cause=null)",
      "ScopeExiting(null)",
      "UnconsumedChecked(origin=scopeCompletion, nonEmpty=true, events=[Complete])",
    )
  }

  // -------------------------------------------------------------------------------------------
  // cancelAndIgnoreRemainingEvents vs cancelAndConsumeRemainingEvents against buffered terminal
  // events. The difference is more than "throws vs does not throw": one drains and returns, the
  // other never reads the buffer.
  // -------------------------------------------------------------------------------------------

  @Test
  fun ignoreCancelOnBufferedCompleteDoesNotDrainAndNeverThrows() = runRecordedTest { recorder ->
    emptyFlow<Nothing>().test {
      cancelAndIgnoreRemainingEvents()
    }

    recorder.assertSubsequence(
      "ChannelClosed(Complete)",
      "CancelRequested(source=cancelAndIgnoreRemainingEvents, openForSend=false)",
      "UnconsumedChecked(origin=ensureAllEventsConsumed, nonEmpty=false, events=[])",
    )
  }

  @Test
  fun ignoreCancelOnBufferedErrorSwallowsTheError() = runRecordedTest { recorder ->
    val expected = RuntimeException("upstream")
    flow<Nothing> { throw expected }
      .test {
        cancelAndIgnoreRemainingEvents()
      }

    recorder.assertSubsequence(
      "ChannelClosed(Error(RuntimeException))",
      "CancelRequested(source=cancelAndIgnoreRemainingEvents, openForSend=false)",
      "UnconsumedChecked(origin=ensureAllEventsConsumed, nonEmpty=false, events=[])",
    )
  }

  @Test
  fun consumeCancelOnBufferedCompleteDrainsAndReturnsComplete() = runRecordedTest { recorder ->
    emptyFlow<Nothing>().test {
      assertEquals(listOf(Event.Complete), cancelAndConsumeRemainingEvents())
    }

    recorder.assertSubsequence(
      "ChannelClosed(Complete)",
      "EventsDrained(Complete)",
      "CancelRequested(source=cancelAndConsumeRemainingEvents, openForSend=false)",
      "UnconsumedChecked(origin=ensureAllEventsConsumed, nonEmpty=false, events=[])",
    )
  }

  @Test
  fun consumeCancelOnBufferedItemsAndErrorDrainsIncludingError() = runRecordedTest { recorder ->
    val expected = RuntimeException("upstream")
    flow {
      emit("one")
      throw expected
    }
      .test {
        val remaining = cancelAndConsumeRemainingEvents()
        assertEquals(listOf(Event.Item("one"), Event.Error(expected)), remaining)
        assertSame(expected, (remaining.last() as Event.Error).throwable)
      }

    recorder.assertSubsequence(
      "ChannelClosed(Error(RuntimeException))",
      "EventsDrained(Item(one), Error(RuntimeException))",
      "CancelRequested(source=cancelAndConsumeRemainingEvents, openForSend=false)",
    )
  }

  @Test
  fun ignoreCancelOnOpenUpstreamCancelsCollectorAndSuppressesTerminal() =
    runRecordedTest { recorder ->
      neverFlow().test { cancelAndIgnoreRemainingEvents() }

      recorder.assertSubsequence(
        "CancelRequested(source=cancelAndIgnoreRemainingEvents, openForSend=true)",
        "ChannelClosed(Error(JobCancellationException))",
        "JobCompleted(cancelled=true, cause=JobCancellationException)",
        "UnconsumedChecked(origin=ensureAllEventsConsumed, nonEmpty=false, events=[])",
      )
    }

  // -------------------------------------------------------------------------------------------
  // Nested turbineScope: the outer registry wins.
  // -------------------------------------------------------------------------------------------

  @Test
  fun nestedTurbineScopeDoesNotInstallSecondRegistry() = runRecordedTest { recorder ->
    turbineScope {
      val outer = neverFlow().testIn(backgroundScope)
      turbineScope {
        val inner = flowOf("one").testIn(backgroundScope)
        assertEquals("one", inner.awaitItem())
        inner.awaitComplete()
      }
      outer.cancel()
    }

    recorder.expectEvents(
      LifecycleEvent.RegistryScopeEntered(nested = false),
      LifecycleEvent.CollectorStarted("testIn"),
      LifecycleEvent.RegistryScopeEntered(nested = true),
      LifecycleEvent.CollectorStarted("testIn"),
      LifecycleEvent.ChannelSent("Item(one)"),
      LifecycleEvent.ChannelClosed("Complete"),
      LifecycleEvent.JobCompleted(cancelled = false, cause = null),
      LifecycleEvent.TimeoutResolved("3s", "Default", "WallClock"),
      LifecycleEvent.AwaitEntered,
      LifecycleEvent.AwaitExited("Item(one)"),
      LifecycleEvent.TimeoutResolved("3s", "Default", "WallClock"),
      LifecycleEvent.AwaitEntered,
      LifecycleEvent.AwaitExited("Complete"),
      LifecycleEvent.CancelRequested("cancel", openForSend = true),
      LifecycleEvent.ChannelClosed("Error(JobCancellationException)"),
      LifecycleEvent.JobCompleted(cancelled = true, cause = "JobCancellationException"),
    )
  }

  // -------------------------------------------------------------------------------------------
  // Timeout provenance: explicit parameter > context element > 3s default.
  // -------------------------------------------------------------------------------------------

  @Test
  fun timeoutDefaultsToThreeSecondsAndWallClockUnderRunTest() = runRecordedTest { recorder ->
    val turbine = recorder.turbine<String>()
    turbine.add("x")
    turbine.awaitItem()

    val resolved = recorder.snapshot().filterIsInstance<LifecycleEvent.TimeoutResolved>().single()
    assertEquals("3s", resolved.timeout)
    assertEquals("Default", resolved.source)
    assertEquals("WallClock", resolved.mechanism)
  }

  @Test
  fun timeoutFromContextElement() = runRecordedTest { recorder ->
    withTurbineTimeout(42.milliseconds) {
      val turbine = recorder.turbine<String>()
      turbine.add("x")
      turbine.awaitItem()
    }

    val resolved = recorder.snapshot().filterIsInstance<LifecycleEvent.TimeoutResolved>().single()
    assertEquals("42ms", resolved.timeout)
    assertEquals("Context", resolved.source)
    assertEquals("WallClock", resolved.mechanism)
  }

  @Test
  fun explicitTurbineParameterBeatsContextAndDefault() = runRecordedTest { recorder ->
    withTurbineTimeout(42.milliseconds) {
      val turbine = recorder.turbine<String>(timeout = 7.milliseconds)
      turbine.add("x")
      turbine.awaitItem()
    }

    val resolved = recorder.snapshot().filterIsInstance<LifecycleEvent.TimeoutResolved>().single()
    assertEquals("7ms", resolved.timeout)
    assertEquals("Explicit", resolved.source)
  }

  @Test
  fun explicitTestParameterBeatsNestedContext() = runRecordedTest { recorder ->
    withTurbineTimeout(42.milliseconds) {
      flowOf("x").test(timeout = 9.milliseconds) {
        awaitItem()
        awaitComplete()
      }
    }

    val resolved =
      recorder.snapshot().filterIsInstance<LifecycleEvent.TimeoutResolved>().also {
        assertTrue(it.isNotEmpty())
      }
    assertTrue(resolved.all { it.timeout == "9ms" }, resolved.toString())
    assertTrue(resolved.all { it.source == "Explicit" }, resolved.toString())
  }

  @Test
  fun explicitTestInParameterBeatsContext() = runRecordedTest { recorder ->
    turbineScope {
      withTurbineTimeout(42.milliseconds) {
        val turbine = flowOf("x").testIn(backgroundScope, timeout = 11.milliseconds)
        turbine.awaitItem()
        turbine.awaitComplete()
        turbine.cancel()
      }
    }

    val resolved = recorder.snapshot().filterIsInstance<LifecycleEvent.TimeoutResolved>()
    assertTrue(resolved.isNotEmpty())
    assertTrue(resolved.all { it.timeout == "11ms" }, resolved.toString())
    assertTrue(resolved.all { it.source == "Explicit" }, resolved.toString())
  }

  // -------------------------------------------------------------------------------------------
  // Await cancellation ownership.
  // -------------------------------------------------------------------------------------------

  @Test
  fun awaitExitsAsCancelledWhenCallerJobCancelled() = runRecordedTest { recorder ->
    val turbine = recorder.turbine<String>()
    val waiter =
      launch(recorder.asElement(), start = CoroutineStart.UNDISPATCHED) {
        runCatching { turbine.awaitItem() }
      }
    waiter.cancel()
    waiter.join()

    recorder.assertSubsequence("AwaitEntered", "AwaitExited(Cancelled)")
  }

  @Test
  fun timeoutMechanismSelectorUsesVirtualTimeWithoutTestScheduler() {
    assertEquals(
      TimeoutMechanism.Virtual,
      TimeoutMechanism.forContext(kotlin.coroutines.EmptyCoroutineContext),
    )
  }
}
