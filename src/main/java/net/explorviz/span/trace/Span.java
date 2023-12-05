package net.explorviz.span.trace;

import com.datastax.oss.driver.api.core.cql.Row;
import java.util.UUID;
import net.explorviz.span.persistence.TimestampHelper;

public record Span(
    UUID landscapeToken, // TODO: Deviation from frontend, expects `String landscapeToken`
    long traceId, // TODO: Deviation from frontend, expects `String traceId`
    long spanId, // TODO: Deviation from frontend, expects `String spanId`
    long parentSpanId, // TODO: Deviation from frontend, expects `String parentSpanId`
    long startTime,
    long endTime,
    String methodHash // TODO: Deviation from frontend, expects `String hashCode`
) {
  public static Span fromRow(final Row row) {
    final UUID landscapeToken = row.getUuid("landscape_token");
    final long traceId = row.getLong("trace_id");
    final long spanId = row.getLong("span_id");
    final long parentSpanId = row.getLong("parent_span_id");
    // TODO: Remove millisecond/nanosecond mismatch hotfix
    final long startTime = TimestampHelper.toNanosTimestamp(
        row.getInt("start_time_s"), row.getInt("start_time_ns")) / 1_000_000L;
    final long endTime = TimestampHelper.toNanosTimestamp(
        row.getInt("end_time_s"), row.getInt("end_time_ns")) / 1_000_000L;
    final String methodHash = String.valueOf(row.getLong("method_hash"));

    return new Span(landscapeToken, traceId, spanId, parentSpanId, startTime, endTime, methodHash);
  }
}
