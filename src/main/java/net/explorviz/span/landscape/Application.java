package net.explorviz.span.landscape;

import java.util.List;

public record Application(
    String name,
    String language,
    int instance, // TODO: Deviation from frontend, expects `String instanceId`
    List<Package> packages
) {
}
