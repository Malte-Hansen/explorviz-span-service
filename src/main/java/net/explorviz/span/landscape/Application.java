package net.explorviz.span.landscape;

import java.util.List;

public record Application(
    String name,
    String language,
    int instanceId,
    List<Package> packages
) {
}
