package net.explorviz.span.landscape;

public record Method(
    String name,
    long methodHash // TODO: Deviation from frontend, expects `String hashCode`
) {
}
