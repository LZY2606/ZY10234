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
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest

/**
 * Timeout precedence and enforcement mechanism.
 *
 * Precedence (highest first): explicit argument > `withTurbineTimeout` context > 3s default.
 *
 * Inside `runTest` the mechanism is always [TimeoutMechanism.WallClock] (the virtual clock must not
 * be consumed by Turbine); outside it is [TimeoutMechanism.VirtualTime]. These tests never wait for
 * a timeout to fire: an item is always immediately available, so they use no real delays.
 */
class LifecycleTimeoutTest {
  @Test
  fun defaultTimeoutResolvedWhenNothingInstalled() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      flowOf("x").test {
        assertEquals("x", awaitItem())
        awaitComplete()
      }
    }

    val resolved = recorder.tracesOfType<LifecycleTrace.TimeoutResolved>().first()
    assertEquals(TimeoutSource.Default, resolved.source)
    assertEquals(3.seconds, resolved.timeout)
    assertEquals(TimeoutMechanism.WallClock, resolved.mechanism)
  }

  @Test
  fun explicitTestArgumentBecomesExplicitSourceInsideBlock() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      flowOf("x").test(timeout = 250.milliseconds) {
        assertEquals("x", awaitItem())
        awaitComplete()
      }
    }

    val resolved = recorder.tracesOfType<LifecycleTrace.TimeoutResolved>().first()
    assertEquals(TimeoutSource.Explicit, resolved.source)
    assertEquals(250.milliseconds, resolved.timeout)
  }

  @Test
  fun contextTimeoutAppliesWhenNoExplicitArgument() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      withTurbineTimeout(400.milliseconds) {
        flowOf("x").test {
          assertEquals("x", awaitItem())
          awaitComplete()
        }
      }
    }

    val resolved = recorder.tracesOfType<LifecycleTrace.TimeoutResolved>().first()
    assertEquals(TimeoutSource.Context, resolved.source)
    assertEquals(400.milliseconds, resolved.timeout)
  }

  @Test
  fun explicitTestArgumentOverridesEnclosingContext() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      withTurbineTimeout(9.seconds) {
        flowOf("x").test(timeout = 250.milliseconds) {
          assertEquals("x", awaitItem())
          awaitComplete()
        }
      }
    }

    val resolved = recorder.tracesOfType<LifecycleTrace.TimeoutResolved>().first()
    assertEquals(TimeoutSource.Explicit, resolved.source)
    assertEquals(250.milliseconds, resolved.timeout)
  }

  @Test
  fun explicitTestInArgumentOverridesEnclosingContext() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      turbineScope {
        withTurbineTimeout(9.seconds) {
          val turbine = flowOf("x").testIn(this, timeout = 250.milliseconds)
          assertEquals("x", turbine.awaitItem())
          turbine.cancelAndIgnoreRemainingEvents()
        }
      }
    }

    val resolved = recorder.tracesOfType<LifecycleTrace.TimeoutResolved>().first()
    assertEquals(TimeoutSource.Explicit, resolved.source)
    assertEquals(250.milliseconds, resolved.timeout)
  }

  @Test
  fun explicitStandaloneTurbineArgumentWins() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      withTurbineTimeout(9.seconds) {
        val turbine = recordedTurbine<String>(recorder, timeout = 250.milliseconds)
        turbine.add("x")
        assertEquals("x", turbine.awaitItem())
        turbine.cancelAndIgnoreRemainingEvents()
      }
    }

    val resolved = recorder.tracesOfType<LifecycleTrace.TimeoutResolved>().first()
    assertEquals(TimeoutSource.Explicit, resolved.source)
    assertEquals(250.milliseconds, resolved.timeout)
  }

  @Test
  fun nonPositiveTimeoutRejectedEagerlyByTestIn() = runTest {
    turbineScope {
      assertFailsWith<IllegalStateException> {
        neverFlow().testIn(this, timeout = 0.milliseconds)
      }
    }
  }
}
