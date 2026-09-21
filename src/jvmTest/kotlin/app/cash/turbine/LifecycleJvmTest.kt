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
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletionHandlerException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest

/**
 * JVM-only assertions about the cause / suppressed-exception relationships that arise when a
 * validation block and an upstream flow both fail.
 */
class LifecycleJvmTest {
  private fun Throwable.suppressedArray(): Array<Throwable> =
    (this as java.lang.Throwable).suppressed

  @Test
  fun blockFailurePropagatesAsCauseWithoutSuppressedExceptions() = runTest {
    val blockFailure = CustomThrowable("block only")

    val actual =
      assertFailsWith<CustomThrowable> {
        neverFlow().test { throw blockFailure }
      }

    assertSame(blockFailure, actual)
    assertContentEquals(emptyArray(), actual.suppressedArray())
  }

  @Test
  fun bufferedUpstreamFailureWrapsBlockFailureAsCauseWithoutSuppressed() = runTest {
    val recorder = LifecycleRecorder()
    val upstream = CustomThrowable("upstream")
    val blockFailure = CustomThrowable("block")

    val actual =
      assertFailsWith<TurbineAssertionError> {
        withLifecycleRecorder(recorder) {
          flow {
            emit("item")
            throw upstream
          }
            .test {
              assertEquals("item", awaitItem())
              throw blockFailure
            }
        }
      }

    // The block failure remains the direct cause; the upstream error is reported in the message
    // rather than attached as a suppressed exception.
    assertSame(blockFailure, actual.cause)
    assertContentEquals(emptyArray(), actual.suppressedArray())
  }

  @Test
  fun bufferedUpstreamErrorAloneIsCauseOfUnconsumedAssertion() = runTest {
    val upstream = CustomThrowable("solo upstream")

    val actual =
      assertFailsWith<TurbineAssertionError> {
        flow<Nothing> { throw upstream }.test {}
      }

    assertSame(upstream, actual.cause)
    assertContentEquals(emptyArray(), actual.suppressedArray())
  }

  @Test
  fun backgroundScopeTeardownRunsCleanUnconsumedCheck() {
    val recorder = LifecycleRecorder()

    TestScope().runTest {
      withLifecycleRecorder(recorder) {
        turbineScope { neverFlow().testIn(backgroundScope) }
      }
    }

    val checks = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>()
    assertTrue(checks.isNotEmpty(), recorder.describe())
    assertTrue(checks.all { it.unconsumed.isEmpty() }, recorder.describe())
  }

  @Test
  fun backgroundScopeTeardownSurfacesBufferedEventsAsCompletionHandlerException() {
    val recorder = LifecycleRecorder()

    val wrapper =
      assertFailsWith<CompletionHandlerException> {
        TestScope().runTest {
          withLifecycleRecorder(recorder) {
            turbineScope {
              launch(start = CoroutineStart.UNDISPATCHED) {
                flowOf("item").testIn(backgroundScope)
              }
            }
          }
        }
      }

    val failure = wrapper.cause as AssertionError
    assertTrue(failure.message!!.contains("Item(item)"), failure.message)
    val check = recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().last()
    assertEquals(listOf(Event.Item("item"), Event.Complete), check.unconsumed)
  }
}
