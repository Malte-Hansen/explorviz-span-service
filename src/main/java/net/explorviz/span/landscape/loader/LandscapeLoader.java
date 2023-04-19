package net.explorviz.span.landscape.loader;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.quarkus.runtime.api.session.QuarkusCqlSession;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Multi;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.enterprise.context.ApplicationScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class LandscapeLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(LandscapeLoader.class);

  private final AtomicLong lastRequestedLandscapes = new AtomicLong(0L);
  private final AtomicLong lastLoadedStructures = new AtomicLong(0L);

  private final QuarkusCqlSession session;

  private final PreparedStatement selectSpanStructure;
  private final PreparedStatement selectSpanStructureByTime;

  public LandscapeLoader(final QuarkusCqlSession session) {
    this.session = session;

    this.selectSpanStructure =
        session.prepare("SELECT * " + "FROM span_structure " + "WHERE landscape_token = ?");
    this.selectSpanStructureByTime = session.prepare(
        "SELECT * " + "FROM span_structure " + "WHERE landscape_token = ? " + "AND time_seen >= ? "
            + "AND time_seen <= ? " + "ALLOW FILTERING");
  }

  public Multi<LandscapeRecord> loadLandscape(final UUID landscapeToken) {
    LOGGER.debug("Loading landscape {}", landscapeToken);

    final BoundStatement stmtSelect = selectSpanStructure.bind(landscapeToken);
    return executeQuery(stmtSelect);
  }

  public Multi<LandscapeRecord> loadLandscape(final UUID landscapeToken, final long from,
      final long to) {
    LOGGER.debug("Loading landscape {} in time range {}-{}", landscapeToken, from, to);

    final BoundStatement stmtSelectByTime =
        selectSpanStructureByTime.bind(landscapeToken, from, to);
    return executeQuery(stmtSelectByTime);
  }

  private Multi<LandscapeRecord> executeQuery(final BoundStatement stmtSelect) {
    lastRequestedLandscapes.incrementAndGet();

    return session.executeReactive(stmtSelect).map(LandscapeRecord::fromRow).onItem()
        .invoke(lastLoadedStructures::incrementAndGet);
  }

  @Scheduled(every = "{explorviz.log.span.interval}")
  public void logStatus() {
    final long loadedLandscapes = this.lastRequestedLandscapes.getAndSet(0);
    final long loadedStructures = this.lastLoadedStructures.getAndSet(0);
    LOGGER.debug("Requested {} landscapes. Loaded {} structures.", loadedLandscapes,
        loadedStructures);
  }
}
