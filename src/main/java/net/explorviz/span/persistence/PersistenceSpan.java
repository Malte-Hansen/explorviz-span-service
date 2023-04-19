package net.explorviz.span.persistence;

import java.util.UUID;

public record PersistenceSpan(
    UUID landscapeToken,
    long spanId,
    long parentSpanId,
    long traceId,
    long startTime,
    long endTime,
    String nodeIpAddress, // TODO: Convert into InetAddress type?
    String applicationName,
    String applicationLanguage,
    int applicationInstance,
    String methodFqn,
    long methodHash
) {
    public static final long NANOS_PER_SEC = 1_000_000_000L;

    public int getStartTimeBucket() {
        return getStartTimeSeconds(); // TODO: Define time bucket
    }

    // TODO: These need to be tested! :S Potentially optimize divideUnsigned call out. Consider negative longs may be valid

    /**
     * Timestamps in 32bit signed ints are no good since Year2038 is so close. We buy some time by shifting the epoch
     * time to -2<sup>31</sup>, therefore having one more bit without breaking monotonicity.
     */
    public int getStartTimeSeconds() {
        return (int) ((Long.divideUnsigned(startTime, NANOS_PER_SEC) + Integer.MIN_VALUE) & 0xFFFFFFFFL);
    }

    public int getStartTimeNanos() {
        return (int) ((startTime & 0xFFFFFFFFL) % NANOS_PER_SEC);
    }
}
