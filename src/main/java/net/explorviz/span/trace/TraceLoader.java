package net.explorviz.span.trace;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.quarkus.runtime.api.session.QuarkusCqlSession;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class TraceLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(TraceLoader.class);

  private final QuarkusCqlSession session;

  private final PreparedStatement selectTraceByTime;
  private final PreparedStatement selectSpanByTraceid;
  private final PreparedStatement selectAllTraces;

  @Inject
  public TraceLoader(final QuarkusCqlSession session) {
    this.session = session;

    // only for debug, must not be used in production due to ALLOW FILTERING
    this.selectAllTraces =
        session.prepare("SELECT * " + "FROM trace_by_time " + "WHERE landscape_token = ? "
            + "ALLOW FILTERING");

    this.selectTraceByTime = session.prepare(
        "SELECT * " + "FROM trace_by_time " + "WHERE landscape_token = ? "
            + "AND tenth_second_epoch = ?");
    this.selectSpanByTraceid = session.prepare(
        "SELECT * " + "FROM span_by_traceid " + "WHERE landscape_token = ? " + "AND trace_id = ?");
  }

  public Multi<Trace> loadAllTraces(final UUID landscapeToken) {
    LOGGER.atTrace().addArgument(landscapeToken).log("Loading all traces for token {}");

    // TODO: Trace should not contain itself? i.e. filter out parent_span_id = 0
    // TODO: Is from/to inclusive/exclusive?
    return session.executeReactive(selectAllTraces.bind(landscapeToken)).map(Trace::fromRow)
        .flatMap(trace -> {
          LOGGER.atTrace().addArgument(() -> trace.traceId()).log("Found trace {}");
          return session.executeReactive(selectSpanByTraceid.bind(landscapeToken, trace.traceId()))
              .map(Span::fromRow).collect().asList().map(spanList -> {
                trace.spanList().addAll(spanList);
                return trace;
              }).toMulti();
        });
  }

  public Multi<Trace> loadTracesStartingInRange(final UUID landscapeToken,
      final long tenthSecondEpoch) {
    LOGGER.atTrace().addArgument(landscapeToken).addArgument(tenthSecondEpoch)
        .log("Loading all traces for token {} in epoch bucket {}");

    // TODO: Trace should not contain itself? i.e. filter out parent_span_id = 0
    // TODO: Is from/to inclusive/exclusive?
    return session.executeReactive(selectTraceByTime.bind(landscapeToken, tenthSecondEpoch))
        .map(Trace::fromRow).flatMap(trace -> {
          LOGGER.atTrace().addArgument(() -> trace.traceId()).log("Found trace {}");
          return session.executeReactive(selectSpanByTraceid.bind(landscapeToken, trace.traceId()))
              .map(Span::fromRow).collect().asList().map(spanList -> {
                trace.spanList().addAll(spanList);
                return trace;
              }).toMulti();
        });
  }

  // TODO: Trace should not contain itself? i.e. filter out parent_span_id = 0

  public Uni<Trace> loadTrace(final UUID landscapeToken, final String traceId) {
    /*
     * LOGGER.atTrace().addArgument(traceId).addArgument(landscapeToken)
     * .log("Loading trace {} for token {}");
     *
     * return session.executeReactive(selectSpanByTraceid.bind(landscapeToken,
     * traceId))
     * .map(Span::fromRow).collect().asList().map(Trace::fromSpanList);
     */
    return Uni.createFrom().nullItem();
  }
}
