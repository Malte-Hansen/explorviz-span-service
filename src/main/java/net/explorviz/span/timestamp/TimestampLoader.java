package net.explorviz.span.timestamp;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.quarkus.runtime.api.session.QuarkusCqlSession;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class TimestampLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimestampLoader.class);

  private final QuarkusCqlSession session;

  private final PreparedStatement selectAllTimestampsForToken;
  private final PreparedStatement selectNewerTimestampsForToken;

  @Inject
  public TimestampLoader(final QuarkusCqlSession session) {
    this.session = session;

    this.selectAllTimestampsForToken = session.prepare(
        "SELECT * " + "FROM span_count_per_time_bucket_and_token "
            + "WHERE landscape_token = ?");

    this.selectNewerTimestampsForToken = session.prepare(
        "SELECT * " + "FROM span_count_per_time_bucket_and_token "
            + "WHERE landscape_token = ? AND ten_second_epoch > ?");
  }

  public Multi<Timestamp> loadAllTimestampsForToken(final UUID landscapeToken) {
    LOGGER.atTrace().addArgument(landscapeToken).log("Loading all timestamps for token {}");

    return session.executeReactive(this.selectAllTimestampsForToken.bind(landscapeToken))
        .map(Timestamp::fromRow);
  }

  public Multi<Timestamp> loadNewerTimestampsForToken(UUID landscapeToken, long newest) {
    LOGGER.atTrace().addArgument(landscapeToken).addArgument(newest)
        .log("Loading newer timestamps for token {} and newest timestamp {}.");

    return session.executeReactive(this.selectNewerTimestampsForToken.bind(landscapeToken, newest))
        .map(Timestamp::fromRow);
  }
}
