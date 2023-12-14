package net.explorviz.span.api;

import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.util.UUID;
import net.explorviz.span.persistence.PersistenceSpan;
import net.explorviz.span.timestamp.Timestamp;
import net.explorviz.span.timestamp.TimestampLoader;

@Path("/v2/landscapes")
@Produces(MediaType.APPLICATION_JSON)
public class TimestampResource {

  @Inject
  TimestampLoader timestampLoader;

  @GET
  @Path("/{token}/timestamps")
  public Multi<Timestamp> getStructure(@PathParam("token") final String token,
      @QueryParam("newest") final long newest, @QueryParam("oldest") final long oldest) {
    if (newest == 0 && oldest == 0) {
      return this.timestampLoader.loadAllTimestampsForToken(parseUuid(token));
    }

    if (newest != 0) {
      return this.timestampLoader.loadNewerTimestampsForToken(parseUuid(token), newest);
    }
    return Multi.createFrom().empty();
  }

  private UUID parseUuid(final String token) {
    // TODO: Remove invalid token hotfix
    if ("mytokenvalue".equals(token)) {
      return PersistenceSpan.DEFAULT_UUID;
    }

    try {
      return UUID.fromString(token);
    } catch (final IllegalArgumentException e) {
      throw new BadRequestException("Invalid token", e);
    }
  }

}
