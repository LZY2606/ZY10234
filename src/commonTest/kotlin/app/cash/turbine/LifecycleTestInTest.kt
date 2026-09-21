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
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext

/**
 * Lifecycle anatomy for `Flow.testIn` across scope lifetimes.
 *
 * Cases that depend on `runTest`'s post-body background-scope teardown timing live in
 * `LifecycleJvmTest` because that teardown surfaces failures synchronously only on JVM; the
 * cross-platform cases below use the explicit-cancel + exception-handler pattern that works on
 * every target.
 */
class LifecycleTestInTest {
  @Test
  fun testInForegroundScopeRunsUnconsumedCheckWhenChildCompletes() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      turbineScope {
        val turbine = flowOf(1).testIn(this)
        assertEquals(1, turbine.awaitItem())
        // Scope still alive: the completion-handler check has not run yet.
        assertTrue(
          recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().isEmpty(),
          recorder.describe(),
        )
        turbine.awaitComplete()
      }
    }

    // Foreground child scope exited normally, so the handler validated consumption and found
    // nothing remaining.
    val checks = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>()
    assertTrue(checks.isNotEmpty(), recorder.describe())
    assertTrue(checks.all { it.unconsumed.isEmpty() }, recorder.describe())
  }

  @Test
  fun testInUnconsumedItemSurfacedWhenCollectorCanceledExternally() = runTest {
    // Mirrors the established cross-platform pattern: an undispatched collector canceled with a
    // buffered item reports it through the enclosing exception handler.
    val recorder = LifecycleRecorder()
    val exceptionHandler = RecordingExceptionHandler()

    withLifecycleRecorder(recorder) {
      turbineScope {
        launch(start = CoroutineStart.UNDISPATCHED) {
            withContext(exceptionHandler) {
              flow {
                  emit("item!")
                  kotlinx.coroutines.awaitCancellation()
                }
                .testIn(this)
            }
          }
          .cancel()
      }
    }

    val wrapper = exceptionHandler.exceptions.removeFirst()
    val cause = wrapper.cause ?: wrapper
    assertTrue(cause is AssertionError, "expected AssertionError but was $wrapper")
    val check = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().last()
    assertEquals(listOf(Event.Item("item!")), check.unconsumed)
  }

  @Test
  fun testInBufferedErrorCauseIdenticalWhenCollectorCanceledExternally() = runTest {
    val recorder = LifecycleRecorder()
    val upstream = CustomThrowable("scoped")
    val exceptionHandler = RecordingExceptionHandler()

    withLifecycleRecorder(recorder) {
      turbineScope {
        launch(start = CoroutineStart.UNDISPATCHED) {
            withContext(exceptionHandler) {
              flow<Nothing> { throw upstream }.testIn(this)
            }
          }
          .cancel()
      }
    }

    val wrapper = exceptionHandler.exceptions.removeFirst()
    val cause = wrapper.cause ?: wrapper
    assertTrue(cause is AssertionError, "expected AssertionError but was $wrapper")
    assertSame(upstream, cause.cause)
  }

  @Test
  fun testInExplicitCancelCancelsCollectJobAndClosesChannel() = runTest {
    val recorder = LifecycleRecorder()
    val sendGate = CompletableDeferred<Unit>()

    withLifecycleRecorder(recorder) {
      turbineScope {
        val turbine =
          flow<Unit> {
              sendGate.await()
            }
            .testIn(this)
        turbine.cancel()
        sendGate.complete(Unit)
      }
    }

    recorder.assertContainsTrace(LifecycleTrace.CancelRequested)
    // The wrapper cancels via channel.cancel(cause = null); the later close carries the job
    // cancellation.
    recorder.assertContainsTrace(LifecycleTrace.ChannelCancelRequested(null))
  }

  @Test
  fun testInRequiresTurbineScopeAndDoesNotStartCollectorWithoutOne() = runTest {
    val failure =
      assertFailsWith<AssertionError> {
        // backgroundScope carries no registry element outside a turbineScope.
        emptyFlow<Nothing>().testIn(backgroundScope)
      }
    assertEquals(
      "Turbine can only collect flows within a TurbineContext. Wrap with turbineScope { .. }",
      failure.message,
    )
  }
}
