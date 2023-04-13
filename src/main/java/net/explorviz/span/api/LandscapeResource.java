package net.explorviz.span.api;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import io.smallrye.mutiny.Uni;
import net.explorviz.span.landscape.Landscape;
import net.explorviz.span.landscape.assembler.LandscapeAssembler;
import net.explorviz.span.landscape.loader.LandscapeLoader;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;

import java.util.UUID;

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
        if (token.equals("mytokenvalue")) {
            token = "7cd8a9a7-b840-4735-9ef0-2dbbfa01c039"; // TODO: Remove invalid token hotfix
        }

        // TODO: from, to
        return loader.loadLandscape(UUID.fromString(token)).collect().asList().map(assembler::assembleFromRecords);
    }
}
