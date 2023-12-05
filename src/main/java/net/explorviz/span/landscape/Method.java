package net.explorviz.span.landscape;

public record Method(
    String name,
    String methodHash // TODO: Deviation from frontend, expects `String hashCode`
) {
}
