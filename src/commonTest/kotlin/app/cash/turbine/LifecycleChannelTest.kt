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
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.test.runTest

/**
 * Lifecycle anatomy for the public `ReceiveChannel` extensions.
 *
 * Unlike `test`/`testIn`, a raw channel extension owns no collector, no collect job, performs no
 * auto-cancel, and runs no unconsumed-event check. It only produces await enter/exit pairs.
 */
class LifecycleChannelTest {
  @Test
  fun channelExtensionAwaitOnlyRecordsAwaitPair() = runTest {
    val recorder = LifecycleRecorder()

    withLifecycleRecorder(recorder) {
      val channel = Channel<String>(UNLIMITED)
      channel.trySend("x")
      val event = channel.awaitEvent()
      assertEquals(Event.Item("x"), event)
      channel.close()
      assertEquals(Event.Complete, channel.awaitEvent())
      channel.cancel()
    }

    recorder.assertNotContainsTrace(LifecycleTrace.CollectorStarted)
    recorder.assertNotContainsTrace(LifecycleTrace.CancelRequested)
    assertTrue(
      recorder.tracesOfType<LifecycleTrace.UnconsumedChecked>().isEmpty(),
      recorder.describe(),
    )
    val enters = recorder.tracesOfType<LifecycleTrace.AwaitEnter>()
    val exits = recorder.tracesOfType<LifecycleTrace.AwaitExit>()
    assertEquals(2, enters.size, recorder.describe())
    assertEquals(2, exits.size, recorder.describe())
    assertEquals(Event.Item("x"), exits[0].event)
    assertEquals(Event.Complete, exits[1].event)
    assertTrue(enters.all { it.mechanism == TimeoutMechanism.WallClock })
    assertTrue(enters.all { it.timeout == 3.seconds })
  }
}
