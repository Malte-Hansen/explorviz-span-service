package net.explorviz.span.service.converter;

import net.explorviz.avro.Span;
import net.explorviz.span.persistence.PersistenceSpan;
import org.apache.kafka.streams.kstream.ValueMapper;

import javax.enterprise.context.ApplicationScoped;
import java.util.UUID;

@ApplicationScoped
public class SpanConverter implements ValueMapper<Span, PersistenceSpan> {
    @Override
    public PersistenceSpan apply(Span span) {
        String landscapeTokenRaw = span.getLandscapeToken();
        if (landscapeTokenRaw.equals("mytokenvalue")) {
            landscapeTokenRaw = "7cd8a9a7-b840-4735-9ef0-2dbbfa01c039"; // TODO: Remove invalid UUID hotfix
        }
        UUID landscapeToken = UUID.fromString(landscapeTokenRaw);

        long spanId = Long.parseUnsignedLong(span.getSpanId(), 16);

        String parentSpanIdRaw = span.getParentSpanId();
        long parentSpanId = 0;
        if (!parentSpanIdRaw.isEmpty()) {
            parentSpanId = Long.parseUnsignedLong(parentSpanIdRaw, 16);
        }

        long traceId = Long.parseUnsignedLong(span.getTraceId().substring(0, 16), 16); // TODO: Truncated trace id?
        long startTime = span.getStartTimeEpochMilli() * 1_000_000L;
        long endTime = span.getEndTimeEpochMilli() * 1_000_000L;
        String hostIpAddress = span.getHostIpAddress();
        String appName = span.getAppName();
        int appInstanceId = Integer.parseInt(span.getAppInstanceId());
        String appLanguage = span.getAppLanguage();
        String methodFqn = span.getFullyQualifiedOperationName();

        long methodHashCode = HashHelper.calculateSpanHash(
            landscapeToken, hostIpAddress, appName, appInstanceId, methodFqn
        );
        return new PersistenceSpan(
            landscapeToken, spanId, parentSpanId, traceId, startTime, endTime, hostIpAddress, appName, appInstanceId,
            appLanguage, methodFqn, methodHashCode
        );
    }
}
