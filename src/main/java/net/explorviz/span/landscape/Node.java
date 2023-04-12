package net.explorviz.span.landscape;

import java.util.List;

public record Node(
    String ipAddress,
    // TODO: Deviation from frontend, missing `String hostName`
    List<Application> applications
) {
}
