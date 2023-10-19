package net.explorviz.span.trace;

import com.datastax.oss.driver.api.core.cql.Row;
import net.explorviz.span.persistence.TimestampHelper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public record Trace(
    UUID landscapeToken,
    long traceId, // TODO: Deviation from frontend, expects `String traceId`
    long startTime,
    long endTime,
    long duration, // TODO: Pointless?
    int overallRequestCount, // TODO: Always 1 for backwards compat for now
    int traceCount, // TODO: Always 1 for backwards compat for now
    List<Span> spanList
) {
  public static Trace fromRow(final Row row) {
    final UUID landscapeToken = row.getUuid("landscape_token");
    final long traceId = row.getLong("trace_id");
    // TODO: Remove millisecond/nanosecond mismatch hotfix
    final long startTime = TimestampHelper.toNanosTimestamp(
        row.getInt("start_time_s"), row.getInt("start_time_ns")) / 1_000_000L;
    final long endTime = TimestampHelper.toNanosTimestamp(
        row.getInt("end_time_s"), row.getInt("end_time_ns")) / 1_000_000L;
    final long duration = endTime - startTime;
    final int overallRequestCount = 1;
    final int traceCount = 1;
    final List<Span> spanList = new ArrayList<>();

    return new Trace(landscapeToken, traceId, startTime, endTime, duration, overallRequestCount,
        traceCount, spanList);
  }

  public static Trace fromSpanList(final List<Span> spans) {
    Optional<Span> root = spans.stream().filter(span -> span.parentSpanId() == 0).findAny();
    if (root.isEmpty()) {
      throw new IllegalArgumentException("No root span found in span list");
    }
    Span span = root.get();

    final UUID landscapeToken = span.landscapeToken();
    final long traceId = span.traceId();
    final long startTime = span.startTime();
    final long endTime = span.endTime();
    final long duration = endTime - startTime;
    final int overallRequestCount = 1;
    final int traceCount = 1;
    // TODO: Trace should not contain itself? i.e. filter out parent_span_id = 0
    final List<Span> spanList = Collections.unmodifiableList(spans);

    return new Trace(landscapeToken, traceId, startTime, endTime, duration, overallRequestCount,
        traceCount, spanList);
  }
}
