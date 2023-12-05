package net.explorviz.span.persistence;

public final class TimestampHelper {
  private static final long NANOS_PER_SEC = 1_000_000_000L;

  private TimestampHelper() {
  }

  // TODO: These need to be tested! :S Potentially optimize divideUnsigned call out.
  //  Consider negative longs may be valid

  /**
   * Timestamps in 32bit signed ints are no good since Year2038 is so close. We buy some time by
   * shifting the epoch time to -2<sup>31</sup>, therefore having one more bit without breaking
   * monotonicity.
   */
  public static int extractAltSeconds(final long time) {
    return (int) ((Long.divideUnsigned(time, NANOS_PER_SEC) + Integer.MIN_VALUE)
        & 0xFFFFFFFFL);
  }

  public static int extractNanos(final long time) {
    return (int) Long.remainderUnsigned(time, NANOS_PER_SEC);
  }

  public static long toNanosTimestamp(final int altSeconds, final int nanos) {
    return (altSeconds - Integer.MIN_VALUE) * NANOS_PER_SEC + nanos;
  }

  public static void main(final String[] args) throws InterruptedException {
    final long time = System.currentTimeMillis() * 1_000_000L;
    System.out.println(time);

    final int altSeconds = extractAltSeconds(time);
    final int nanos = extractNanos(time);
    System.out.println(altSeconds);
    System.out.println(nanos);
    //Thread.sleep(1000);
    //time = System.currentTimeMillis() * 1_000_000L;
    //System.out.println(extractAltSeconds(time));
    //System.out.println(extractNanos(time));

    System.out.println(toNanosTimestamp(altSeconds, nanos));
  }
}
