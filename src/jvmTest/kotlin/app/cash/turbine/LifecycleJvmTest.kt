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
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.withContext

/** JVM-only lifecycle assertions: real clock virtual-time mechanism and cause/suppressed links. */
class LifecycleJvmTest {
  @Test
  fun timeoutMechanismIsVirtualWithoutTestScheduler() = runBlocking {
    // Item is buffered up front, so the await never waits; its resolved timeout is still observed.
    val recorder = LifecycleRecorder()
    val turbine = recordingTurbine<String>(recorder)
    turbine.add("x")
    withContext(recorder.asElement()) { turbine.awaitItem() }

    val resolved = recorder.snapshot().filterIsInstance<LifecycleEvent.TimeoutResolved>().single()
    assertEquals("3s", resolved.timeout)
    assertEquals("Default", resolved.source)
    assertEquals("Virtual", resolved.mechanism)
  }

  @Test
  fun wallClockTimeoutAssertionWrapsTurbineTimeoutCancellationAsCause() = runBlocking {
    // The shortest explicit positive timeout: this is the only test that waits on a real clock
    // because the WallClock mechanism specifically ignores virtual time.
    val recorder = LifecycleRecorder()
    val error =
      assertFailsWith<AssertionError> {
        withContext(TestCoroutineScheduler()) {
          withContext(recorder.asElement()) {
            val turbine = recordingTurbine<Unit>(recorder, timeout = 1.milliseconds)
            turbine.awaitItem()
          }
        }
      }
    assertEquals("No value produced in 1ms", error.message)
    assertTrue(
      error.cause is TurbineTimeoutCancellationException,
      "expected TurbineTimeoutCancellationException cause, got ${error.cause}",
    )
    val resolved = recorder.snapshot().filterIsInstance<LifecycleEvent.TimeoutResolved>().single()
    assertEquals("1ms", resolved.timeout)
    assertEquals("Explicit", resolved.source)
    assertEquals("WallClock", resolved.mechanism)
  }

  @Test
  fun explicitTimeoutWithWallClockMechanismStillResolvesExplicit() = runBlocking {
    val recorder = LifecycleRecorder()
    withContext(TestCoroutineScheduler()) {
      withContext(recorder.asElement()) {
        val turbine = recordingTurbine<String>(recorder, timeout = 250.milliseconds)
        turbine.add("x")
        turbine.awaitItem()
      }
    }
    val resolved = recorder.snapshot().filterIsInstance<LifecycleEvent.TimeoutResolved>().single()
    assertEquals("250ms", resolved.timeout)
    assertEquals("Explicit", resolved.source)
    assertEquals("WallClock", resolved.mechanism)
  }

  @Test
  fun scopeCancellationSuppressesUnconsumedEventAssertion() = runBlocking {
    val recorder = LifecycleRecorder()
    val handler = RecordingExceptionHandler()
    val scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.Job() + handler)
    val job =
      scope.launch(start = CoroutineStart.UNDISPATCHED) {
        withContext(recorder.asElement()) {
          turbineScope {
            flow {
                emit("item!")
                kotlinx.coroutines.awaitCancellation()
              }
              .testIn(this)
          }
        }
      }
    job.cancel()
    job.join()

    val reported = handler.exceptions.single()
    assertTrue(reported is CompletionHandlerException, "got $reported")
    val cause = reported.cause
    assertTrue(cause is AssertionError, "got $cause")
    assertEquals(
      """
      |Unconsumed events found:
      | - Item(item!)
      """
        .trimMargin(),
      cause.message,
    )

    // The lifecycle assertion does not replace the cancellation: it is reported through the
    // completion handler (observed here) while the cancelled job exposes a plain cancellation
    // exception, not the assertion. Assert the exact relationship rather than a suppressed link
    // that the coroutines runtime does not create in this path.
    val cancellation = job.javaClass.getMethod("getCancellationException").invoke(job) as Throwable
    assertTrue(cancellation is kotlinx.coroutines.CancellationException)
    assertEquals(0, cancellation.suppressedExceptions.size)
    assertSame(cause, reported.cause)
  }

  @Test
  fun unconsumedUpstreamErrorAssertionCarriesErrorAsCauseNotSuppressed() = runBlocking {
    val expected = RuntimeException("upstream")
    val error =
      assertFailsWith<AssertionError> {
        withContext(LifecycleSinkElement(NopSink)) {
          flow<Nothing> { throw expected }.test {}
        }
      }
    assertSame(expected, error.cause)
    assertEquals(0, error.suppressedExceptions.size)
  }

  @Test
  fun validationBlockFailureWithUpstreamErrorWrapsButKeepsBothVisible() = runBlocking {
    val expected = CustomThrowable("validation")
    val upstream = RuntimeException("upstream")
    val error =
      assertFailsWith<AssertionError> {
        withContext(LifecycleSinkElement(NopSink)) {
          flow<Nothing> { throw upstream }.test { throw expected }
        }
      }
    // The validation exception is the cause; the upstream error is rendered into the message.
    assertSame(expected, error.cause)
    assertTrue(error.message!!.contains("RuntimeException: upstream"), error.message)
  }

  private object NopSink : LifecycleSink {
    override fun record(event: LifecycleEvent) = Unit
  }
}
