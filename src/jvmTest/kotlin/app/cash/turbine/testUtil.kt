package app.cash.turbine

actual fun assertCallSitePresentInStackTraceOnJvm(
  throwable: Throwable,
  entryPoint: String,
  callSite: String,
) {
  val lines = throwable.stackTraceToString().lines()

  val awaitItemIndex = lines.indexOfFirst { entryPoint in it }
  if (awaitItemIndex == -1) {
    throw AssertionError("'$entryPoint' not found in stacktrace\n\n${lines.joinToString("\n")}")
  }

  // Internal Turbine coroutine frames (e.g. lifecycle wrappers) may sit between the entry point
  // and the call site. Require the call site to be the nearest subsequent _user_ frame rather
  // than the literally adjacent frame.
  val followingFrames = lines.subList(awaitItemIndex + 1, lines.size)
  val callSiteIndex = followingFrames.indexOfFirst { callSite in it }
  if (
    callSiteIndex == -1 ||
      followingFrames.subList(0, callSiteIndex).any { "app.cash.turbine." !in it }
  ) {
    throw AssertionError(
      "Expected '$callSite' to be the nearest user frame after '$entryPoint', but it was not\n\n${lines.joinToString("\n")}"
    )
  }
}
