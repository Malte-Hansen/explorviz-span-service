package net.explorviz.span.trace;

import com.datastax.oss.driver.api.core.cql.Row;
import java.util.UUID;

public record Span(
    //UUID landscapeToken, // TODO: Deviation from frontend, expects `String landscapeToken`
    //String traceId,
    String spanId,
    String parentSpanId,
    long startTime,
    long endTime,
    String methodHash
) {

  public static Span fromRow(final Row row) {
    //final UUID landscapeToken = row.getUuid("landscape_token");
    //final String traceId = row.getString("trace_id");
    final String spanId = row.getString("span_id");
    final String parentSpanId = row.getString("parent_span_id");
    final long startTime = row.getLong("start_time");
    final long endTime = row.getLong("end_time");
    final String methodHash = row.getString("method_hash");

    return new Span(spanId, parentSpanId, startTime, endTime, methodHash);
  }
}
