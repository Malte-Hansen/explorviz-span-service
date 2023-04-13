package net.explorviz.span.landscape.loader;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.quarkus.runtime.api.session.QuarkusCqlSession;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Multi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class LandscapeLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(LandscapeLoader.class);

    private final AtomicLong lastRequestedLandscapes = new AtomicLong(0L);
    private final AtomicLong lastLoadedStructures = new AtomicLong(0L);

    private final QuarkusCqlSession session;

    private final PreparedStatement selectSpanStructure;

    public LandscapeLoader(QuarkusCqlSession session) {
        this.session = session;

        this.selectSpanStructure = session.prepare(
            "SELECT * "
                + "FROM span_structure "
                + "WHERE landscape_token = ?"
        );
    }

    // TODO: Cache (shared with PersistenceSpanProcessor?)

    public Multi<LandscapeRecord> loadLandscape(UUID landscapeToken) {
        LOGGER.debug("Loading landscape with token {}", landscapeToken);
        lastRequestedLandscapes.incrementAndGet();

        BoundStatement stmtSelect = selectSpanStructure.bind(landscapeToken);
        return session.executeReactive(stmtSelect)
                   .map(LandscapeRecord::fromRow)
                   .onItem().invoke(lastLoadedStructures::incrementAndGet);
    }

    @Scheduled(every = "{explorviz.log.span.interval}")
    public void logStatus() {
        final long loadedLandscapes = this.lastRequestedLandscapes.getAndSet(0);
        final long loadedStructures = this.lastLoadedStructures.getAndSet(0);
        LOGGER.debug("Requested {} landscapes. Loaded {} structures.", loadedLandscapes, loadedStructures);
    }
}
