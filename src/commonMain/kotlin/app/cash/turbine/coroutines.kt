/*
 * Copyright (C) 2022 Square, Inc.
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
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.withContext

private val DEFAULT_TIMEOUT: Duration = 3000.milliseconds

/** Where the effective timeout of a single await came from, strongest priority first. */
internal enum class TimeoutSource {
  /**
   * Supplied directly to `test`, `testIn`, `Turbine`, or `withTurbineTimeout`-style internal
   * wrappers for an explicit parameter.
   */
  Explicit,

  /** Supplied by a `withTurbineTimeout` context element. */
  Context,

  /** The built-in 3 second default. */
  Default,
}

/** Which timing mechanism an await uses once a timeout is resolved. */
internal enum class TimeoutMechanism {
  /**
   * A `TestCoroutineScheduler` is present: virtual time would never elapse, so a wall-clock timeout
   * on `Dispatchers.Default` is used instead.
   */
  WallClock,

  /** No test scheduler: a normal `withTimeout` driven by the coroutine clock. */
  Virtual;

  public companion object {
    internal fun forContext(context: CoroutineContext): TimeoutMechanism =
      if (context[TestCoroutineScheduler] != null) WallClock else Virtual
  }
}

internal fun checkTimeout(timeout: Duration) {
  check(timeout.isPositive()) { "Turbine timeout must be greater than 0: $timeout" }
}

/**
 * Sets a timeout for all [Turbine] instances within this context. If this timeout is not set, the
 * default value is 3sec.
 */
public suspend fun <T> withTurbineTimeout(
  timeout: Duration,
  block: suspend CoroutineScope.() -> T,
): T {
  checkTimeout(timeout)
  return withContext(TurbineTimeoutElement(timeout, TimeoutSource.Context), block)
}

/**
 * Internal variant used to install an explicit `test`/`testIn`/`Turbine` parameter so lifecycle
 * observers can distinguish it from a user-installed context element.
 */
internal suspend fun <T> withTurbineTimeout(
  timeout: Duration,
  source: TimeoutSource,
  block: suspend CoroutineScope.() -> T,
): T {
  checkTimeout(timeout)
  return withContext(TurbineTimeoutElement(timeout, source), block)
}

/**
 * Resolves the effective timeout for an await. An instance parameter beats context beats default.
 */
internal fun resolveTimeout(
  explicit: Duration?,
  context: CoroutineContext,
): Pair<Duration, TimeoutSource> {
  val element = context[TurbineTimeoutElement.Key]
  return when {
    explicit != null -> explicit to TimeoutSource.Explicit
    element != null -> element.timeout to element.source
    else -> DEFAULT_TIMEOUT to TimeoutSource.Default
  }
}

/**
 * Invoke this method to throw an error when your method is not being called by a suspend fun.
 *
 * This is usually used to prevent the usage of shared memory to communicate with code under test in
 * coroutines tests.
 * [Communicating with shared memory is a bad idea](https://go.dev/blog/codelab-share).
 *
 * Concrete example:
 * ```
 * fun takeLastScreen(): Screen {
 *   assertCallingContextIsNotSuspended()
 *
 *   return screens.takeValue()
 * }
 *
 * @Test
 * fun myTest() = runBlocking {
 *   assertCallingContextIsNotSuspended() // fine
 *   takeLastScreen() // boom!
 * }
 * ```
 */
internal fun assertCallingContextIsNotSuspended() {
  val stackTrace = Exception().stackTraceToString()
  // TODO: support non-JVM
  if ("invokeSuspend" in stackTrace) {
    error("Calling context is suspending; use a suspending method instead")
  }
}

internal class TurbineRegistryElement(val registry: MutableList<ChannelTurbine<*>>) :
  CoroutineContext.Element {
  companion object Key : CoroutineContext.Key<TurbineRegistryElement>

  override val key: CoroutineContext.Key<*> = Key
}

/**
 * Internal tool to report turbines that have been spun up within a given scope.
 *
 * If reportTurbines is nested within another reportTurbines, the outer scope wins: no turbines will
 * be registered from the inner scope.
 */
internal suspend fun <T> reportTurbines(
  registry: MutableList<ChannelTurbine<*>>,
  block: suspend () -> T,
): T {
  val enclosingRegistryElement = currentCoroutineContext()[TurbineRegistryElement]
  currentCoroutineContext()
    .lifecycleSink
    ?.record(LifecycleEvent.RegistryScopeEntered(nested = enclosingRegistryElement != null))
  return if (enclosingRegistryElement != null) {
    block()
  } else {
    withContext(TurbineRegistryElement(registry)) { block() }
  }
}

internal fun CoroutineScope.reportTurbine(turbine: ChannelTurbine<*>) =
  coroutineContext[TurbineRegistryElement]?.registry?.add(turbine)

internal class TurbineTimeoutElement(
  val timeout: Duration,
  val source: TimeoutSource,
) : CoroutineContext.Element {
  companion object Key : CoroutineContext.Key<TurbineTimeoutElement>

  override val key: CoroutineContext.Key<*> = Key
}

internal suspend fun contextTimeout(): Duration =
  resolveTimeout(explicit = null, currentCoroutineContext()).first
