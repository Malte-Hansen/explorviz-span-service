package net.explorviz.span.timestamp;

import com.datastax.oss.driver.api.core.cql.Row;

public record Timestamp(long epochMilli, long spanCount) {
  public static Timestamp fromRow(final Row row) {
    final long tenSecondEpoch = row.getLong("ten_second_epoch");
    final long spanCount = row.getLong("span_count");

    return new Timestamp(tenSecondEpoch, spanCount);
  }
}
