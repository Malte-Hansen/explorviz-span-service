package net.explorviz.span.persistence;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.UUID;
import net.explorviz.avro.Span;
import net.explorviz.span.hash.HashHelper;
import org.apache.kafka.streams.kstream.ValueMapper;

@ApplicationScoped
public class SpanConverter implements ValueMapper<Span, PersistenceSpan> {

  @Override
  public PersistenceSpan apply(final Span span) {
    String landscapeTokenRaw = span.getLandscapeToken();
    // TODO: Remove invalid UUID hotfix
    UUID landscapeToken = PersistenceSpan.DEFAULT_UUID;
    if (!"mytokenvalue".equals(landscapeTokenRaw)) {
      landscapeToken = UUID.fromString(landscapeTokenRaw);
    }

    final long spanId = Long.parseUnsignedLong(span.getSpanId(), 16);

    final String parentSpanIdRaw = span.getParentSpanId();
    long parentSpanId = 0;
    if (!parentSpanIdRaw.isEmpty()) {
      parentSpanId = Long.parseUnsignedLong(parentSpanIdRaw, 16);
    }

    final long traceId =
        Long.parseUnsignedLong(span.getTraceId().substring(0, 16), 16); // TODO: Truncated trace id?
    final long startTime = span.getStartTimeEpochMilli() * 1_000_000L;
    final long endTime = span.getEndTimeEpochMilli() * 1_000_000L;
    final String nodeIpAddress = span.getHostIpAddress();
    final String applicationName = span.getAppName();
    final int applicationInstance = Integer.parseInt(span.getAppInstanceId());
    final String applicationLanguage = span.getAppLanguage();
    final String methodFqn = span.getFullyQualifiedOperationName();

    final long methodHashCode =
        HashHelper.calculateSpanHash(landscapeToken, nodeIpAddress, applicationName,
            applicationInstance, methodFqn);

    return new PersistenceSpan(landscapeToken, spanId, parentSpanId, traceId, startTime, endTime,
        nodeIpAddress, applicationName, applicationLanguage, applicationInstance, methodFqn,
        methodHashCode);
  }
}
