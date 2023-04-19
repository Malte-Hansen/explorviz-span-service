package net.explorviz.span.api;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.UUID;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import net.explorviz.span.landscape.Landscape;
import net.explorviz.span.landscape.assembler.LandscapeAssembler;
import net.explorviz.span.landscape.assembler.LandscapeAssemblyException;
import net.explorviz.span.landscape.assembler.impl.NoRecordsException;
import net.explorviz.span.landscape.loader.LandscapeLoader;
import net.explorviz.span.landscape.loader.LandscapeRecord;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;

@Path("/v2/landscapes")
@Produces(MediaType.APPLICATION_JSON)
public class LandscapeResource {
  @Inject
  public LandscapeLoader loader;

  @Inject
  public LandscapeAssembler assembler;

  @GET
  @Path("/{token}/structure")
  @Operation(summary = "Retrieve a landscape graph",
      description = "Assembles the (possibly empty) landscape of "
          + "all spans observed in the given time range")
  @APIResponses(@APIResponse(responseCode = "200", description = "Success",
      content = @Content(mediaType = "application/json",
          schema = @Schema(implementation = Object.class))))
  public Uni<Landscape> getLandscape(@PathParam("token") String token,
      @QueryParam("from") final Long from, @QueryParam("to") final Long to) {
    if ("mytokenvalue".equals(token)) {
      token = "7cd8a9a7-b840-4735-9ef0-2dbbfa01c039"; // TODO: Remove invalid token hotfix NOPMD
    }

    final Multi<LandscapeRecord> recordMulti;
    // TODO: Determine if we should allow from/to alone
    if (from == null || to == null) {
      // TODO: Cache (shared with PersistenceSpanProcessor?)
      recordMulti = loader.loadLandscape(UUID.fromString(token));
    } else {
      recordMulti = loader.loadLandscape(UUID.fromString(token), from, to);
    }

    return recordMulti.collect().asList().map(assembler::assembleFromRecords)
        .onFailure(NoRecordsException.class)
        .transform(t -> new NotFoundException("Landscape not found or empty", t))
        .onFailure(LandscapeAssemblyException.class).transform(
            t -> new InternalServerErrorException("Landscape assembly error: " + t.getMessage(),
                t));
  }
}
