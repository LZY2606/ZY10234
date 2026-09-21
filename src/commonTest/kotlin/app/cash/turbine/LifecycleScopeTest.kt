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
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext

/**
 * Lifecycle anatomy for parent-scope cancellation and nested `turbineScope`s.
 *
 * Nested scopes follow the "outermost registry wins" rule: turbines created in an inner
 * `turbineScope` are registered with (and reported by) the enclosing scope.
 */
class LifecycleScopeTest {
  @Test
  fun parentScopeCancellationRunsUnconsumedCheckForBufferedItem() = runTest {
    val recorder = LifecycleRecorder()
    val exceptionHandler = RecordingExceptionHandler()

    withLifecycleRecorder(recorder) {
      turbineScope {
        launch(start = CoroutineStart.UNDISPATCHED) {
            withContext(exceptionHandler) {
              flow {
                  emit("buffered")
                  kotlinx.coroutines.awaitCancellation()
                }
                .testIn(this)
            }
          }
          .cancel()
      }
    }

    val exception = exceptionHandler.exceptions.removeFirst()
    val cause = exception.cause ?: exception
    assertTrue(cause is AssertionError, "expected AssertionError but was $exception")
    val check = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().last()
    assertEquals(listOf(Event.Item("buffered")), check.unconsumed)
  }

  @Test
  fun cancellationOfNeverFlowLeavesNothingUnconsumed() = runTest {
    val recorder = LifecycleRecorder()
    val exceptionHandler = RecordingExceptionHandler()

    withLifecycleRecorder(recorder) {
      turbineScope {
        val job =
          launch(start = CoroutineStart.UNDISPATCHED) {
            withContext(exceptionHandler) { neverFlow().testIn(this) }
          }
        job.cancel()
      }
    }

    // Never emitted and canceled: the completion-handler check still runs and finds nothing.
    val check = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().last()
    assertEquals(emptyList(), check.unconsumed, recorder.describe())
    assertTrue(exceptionHandler.exceptions.isEmpty())
  }

  @Test
  fun nestedTurbineScopeRegistersWithOutermostRegistry() = runTest {
    val recorder = LifecycleRecorder()
    val inner = CustomThrowable("inner flow")

    val error = runCatching {
      withLifecycleRecorder(recorder) {
        turbineScope {
          turbineScope {
            // Created in the inner block but reported by the outer registry on failure.
            flow<Nothing> { throw inner }.testIn(backgroundScope, name = "inner-turbine")

            // Force the outer scope to fail so it aggregates registered turbine causes.
            throw CustomThrowable("outer block")
          }
        }
      }
    }
      .exceptionOrNull()

    assertTrue(error != null, "expected a failure")
    // The outer block failure propagates; the inner turbine's cause is aggregated into the
    // assertion message.
    val message = error.message.orEmpty()
    assertTrue("inner-turbine" in message, message)
    val checked = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>()
    assertTrue(checked.isNotEmpty(), recorder.describe())
  }

  @Test
  fun nestedTurbineScopeSharesTimeoutContext() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      withTurbineTimeout(123.milliseconds) {
        turbineScope {
          turbineScope {
            val turbine = flowOf("x").testIn(this)
            assertEquals("x", turbine.awaitItem())
            turbine.cancelAndIgnoreRemainingEvents()
          }
        }
      }
    }

    val resolved = recorder.tracesOfType<LifecycleTrace.TimeoutResolved>().first()
    assertEquals(TimeoutSource.Context, resolved.source)
    assertEquals(123.milliseconds, resolved.timeout)
  }
}
