package net.explorviz.span.api;

import javax.ws.rs.core.Application;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.info.Info;

@OpenAPIDefinition(info = @Info(title = "ExplorViz Span API",
    description = "Exposes endpoints to retrieve spans stored in this ExplorViz instance.",
    version = "2.0"))
public class V2APIApplication extends Application {
}
