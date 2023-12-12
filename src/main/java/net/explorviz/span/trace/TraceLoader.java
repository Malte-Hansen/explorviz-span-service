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

    this.selectAllTraces = session.prepare(
        "SELECT * " + "FROM trace_by_time " + "WHERE landscape_token = ? " + "ALLOW FILTERING");
    this.selectTraceByTime = session.prepare(
        "SELECT * " + "FROM trace_by_time " + "WHERE landscape_token = ? " + "AND start_time >= ? "
            + "AND start_time <= ? " + "ALLOW FILTERING");
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

  public Multi<Trace> loadTracesStartingInRange(final UUID landscapeToken, final long from,
      final long to) {
    LOGGER.atTrace().addArgument(landscapeToken).addArgument(from).addArgument(to)
        .log("Loading all traces for token {} in range {} to {}");

    // TODO: Trace should not contain itself? i.e. filter out parent_span_id = 0
    // TODO: Is from/to inclusive/exclusive?
    return session.executeReactive(selectTraceByTime.bind(landscapeToken, from,
        // TimestampHelper.extractAltSeconds(to + 999_999_999L)
        to)).map(Trace::fromRow)
        // why this? isnt this equal to the range above?
        // .filter(trace -> trace.startTime() >= from &&
        // trace.startTime() <= to)
        .flatMap(trace -> {
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
