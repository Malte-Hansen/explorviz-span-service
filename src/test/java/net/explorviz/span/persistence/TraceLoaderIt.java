package net.explorviz.span.persistence;

import com.datastax.oss.quarkus.test.CassandraTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import java.util.List;
import javax.inject.Inject;
import net.explorviz.span.kafka.KafkaTestResource;
import net.explorviz.span.trace.Span;
import net.explorviz.span.trace.Trace;
import net.explorviz.span.trace.TraceLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(KafkaTestResource.class)
@QuarkusTestResource(CassandraTestResource.class)
public class TraceLoaderIt {

  @Inject
  PersistenceSpanProcessor spanProcessor;

  @Inject
  TraceLoader traceLoader;

  private void assertEqualsSpanToPersistenceSpan(PersistenceSpan ps, Span s) {

    final boolean landscapeToken = ps.landscapeToken().equals(s.landscapeToken());
    final boolean spanId = ps.spanId() == s.spanId();
    final boolean parentSpanId = ps.parentSpanId() == s.parentSpanId();
    final boolean traceId = ps.traceId() == s.traceId();
    final boolean startTime = ps.startTime() == s.startTime() * 1_000_000;
    final boolean endTime = ps.endTime() == s.endTime() * 1_000_000;
    final boolean methodHash = s.methodHash().equals(String.valueOf(ps.methodHash()));

    Assertions.assertTrue(landscapeToken);
    Assertions.assertTrue(spanId);
    Assertions.assertTrue(parentSpanId);
    Assertions.assertTrue(traceId);
    Assertions.assertTrue(startTime);
    Assertions.assertTrue(endTime);
    Assertions.assertTrue(methodHash);
  }

  private Span convertPersistenceSpanToSpan(PersistenceSpan ps) {
    return new Span(ps.landscapeToken(), ps.traceId(), ps.spanId(), ps.parentSpanId(),
        ps.startTime() * 1_000_000, ps.endTime() * 1_000_000, String.valueOf(ps.methodHash()));
  }

  @Test
  void testLoadByTraceId() {

    final PersistenceSpan newSpan =
        new PersistenceSpan(PersistenceSpan.DEFAULT_UUID, 123L, 0L, 456L, 1701081838586000000L,
            1701081938586000000L, "nodeIp", "app-name", "java", 0, "net.explorviz.Class.myMethod()",
            847);

    spanProcessor.accept(newSpan);

    Trace result = traceLoader.loadTrace(PersistenceSpan.DEFAULT_UUID, 456L).await().indefinitely();

    Assertions.assertEquals(456L, result.traceId());
  }

  @Test
  void testLoadTracesByTimeRange() {

    long startEarly = 1701081827000000000L;
    long endEarly = 1701081828000000000L;
    long startExpected = 1701081830000000000L;
    long endExpected = 1701081831000000000L;
    long startLate = 1701081833000000000L;
    long endLate = 1701081834000000000L;

    final PersistenceSpan earlySpan =
        new PersistenceSpan(PersistenceSpan.DEFAULT_UUID, 123L, 0L, 456L, startEarly,
            endEarly, "nodeIp", "app-name", "java", 0, "net.explorviz.Class.myMethod()",
            847);

    final PersistenceSpan expectedSpan =
        new PersistenceSpan(PersistenceSpan.DEFAULT_UUID, 123L, 0L, 456L, startExpected,
            endExpected, "nodeIp", "app-name", "java", 0, "net.explorviz.Class.myMethod()",
            847);

    final PersistenceSpan lateSpan =
        new PersistenceSpan(PersistenceSpan.DEFAULT_UUID, 123L, 0L, 456L, startLate,
            endLate, "nodeIp", "app-name", "java", 0, "net.explorviz.Class.myMethod()",
            847);

    spanProcessor.accept(earlySpan);
    spanProcessor.accept(expectedSpan);
    spanProcessor.accept(lateSpan);

    List<Trace> result = traceLoader.loadTraces(PersistenceSpan.DEFAULT_UUID, 1701081828000000000L,
        1701081832000000000L).collect().asList().await().indefinitely();

    Assertions.assertEquals(1, result.size(), "List of traces has wrong size.");
    Assertions.assertEquals(convertPersistenceSpanToSpan(expectedSpan), result.get(0).spanList().get(0), "Wrong span in trace.");
  }

}
