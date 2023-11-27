package net.explorviz.span.persistence;

import static net.explorviz.span.persistence.TimestampHelper.toNanosTimestamp;

import net.explorviz.span.persistence.TimestampHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TimestampHelperTest {

  @Test
  void testExtractAltSeconds() {
    // milliseconds with nano adjustment
    long time = 1701081838586000000L;

    // epoch millisecond
    int expected = -446401810;

    int result = TimestampHelper.extractAltSeconds(time);

    Assertions.assertEquals(expected, result, "Shift operation did not result in correct result.");
  }

  @Test
  void testExtractNanoSeconds() {
    // milliseconds with nano adjustment
    long time = 1701081838586000000L;

    // epoch millisecond
    int expected = 586000000;

    int result = TimestampHelper.extractNanos(time);

    Assertions.assertEquals(expected, result, "Nano extract operation did not result in correct result.");
  }

  @Test
  void testConversion() {

    long expected = 1701081838586000000L;

    // epoch millisecond
    int millis = -446401810;
    int nanos = 586000000;

    long result = TimestampHelper.toNanosTimestamp(millis, nanos);

    Assertions.assertEquals(expected, result, "Timestamp conversion did not result in correct result.");
  }


}
