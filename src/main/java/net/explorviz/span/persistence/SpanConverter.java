package net.explorviz.span.persistence;

import java.util.UUID;
import javax.enterprise.context.ApplicationScoped;
import net.explorviz.avro.Span;
import net.explorviz.span.hash.HashHelper;
import org.apache.kafka.streams.kstream.ValueMapper;

@ApplicationScoped
public class SpanConverter implements ValueMapper<Span, PersistenceSpan> {
  @Override
  public PersistenceSpan apply(Span span) {
    String landscapeTokenRaw = span.getLandscapeToken();
    if (landscapeTokenRaw.equals("mytokenvalue")) {
      landscapeTokenRaw =
          "7cd8a9a7-b840-4735-9ef0-2dbbfa01c039"; // TODO: Remove invalid UUID hotfix
    }
    UUID landscapeToken = UUID.fromString(landscapeTokenRaw);

    long spanId = Long.parseUnsignedLong(span.getSpanId(), 16);

    String parentSpanIdRaw = span.getParentSpanId();
    long parentSpanId = 0;
    if (!parentSpanIdRaw.isEmpty()) {
      parentSpanId = Long.parseUnsignedLong(parentSpanIdRaw, 16);
    }

    long traceId =
        Long.parseUnsignedLong(span.getTraceId().substring(0, 16), 16); // TODO: Truncated trace id?
    long startTime = span.getStartTimeEpochMilli() * 1_000_000L;
    long endTime = span.getEndTimeEpochMilli() * 1_000_000L;
    String nodeIpAddress = span.getHostIpAddress();
    String applicationName = span.getAppName();
    int applicationInstance = Integer.parseInt(span.getAppInstanceId());
    String applicationLanguage = span.getAppLanguage();
    String methodFqn = span.getFullyQualifiedOperationName();

    long methodHashCode =
        HashHelper.calculateSpanHash(landscapeToken, nodeIpAddress, applicationName,
            applicationInstance, methodFqn);
    return new PersistenceSpan(landscapeToken, spanId, parentSpanId, traceId, startTime, endTime,
        nodeIpAddress, applicationName, applicationLanguage, applicationInstance, methodFqn,
        methodHashCode);
  }
}
