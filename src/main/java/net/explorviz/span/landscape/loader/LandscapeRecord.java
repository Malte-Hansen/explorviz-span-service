package net.explorviz.span.landscape.loader;

import com.datastax.oss.driver.api.core.cql.Row;
import java.util.Arrays;
import java.util.UUID;

// TODO: Unify PersistenceSpan and LandscapeRecord? (move FQN parsing into assembler?)
public record LandscapeRecord(UUID landscapeToken, long methodHash, String nodeIpAddress,
                              String applicationName, String applicationLanguage,
                              int applicationInstance, String packageName, String className,
                              String methodName, long timeSeen) {
  public static LandscapeRecord fromRow(Row row) {
    UUID landscapeToken = row.getUuid("landscape_token");
    long methodHash = row.getLong("method_hash");
    String nodeIpAddress = row.getString("node_ip_address");
    String applicationName = row.getString("application_name");
    String applicationLanguage = row.getString("application_language");
    int applicationInstance = row.getInt("application_instance");
    String methodFqn = row.getString("method_fqn");
    long timeSeen = row.getLong("time_seen");

    // TODO: Error handling
    /*
     * By definition getFullyQualifiedOperationName().split("."): Last entry is method name, next to
     * last is class name, remaining elements form the package name
     */
    final String[] operationFqnSplit = methodFqn.split("\\.");

    final String packageName =
        String.join(".", Arrays.copyOf(operationFqnSplit, operationFqnSplit.length - 2));
    final String className = operationFqnSplit[operationFqnSplit.length - 2];
    final String methodName = operationFqnSplit[operationFqnSplit.length - 1];

    return new LandscapeRecord(landscapeToken, methodHash, nodeIpAddress, applicationName,
        applicationLanguage, applicationInstance, packageName, className, methodName, timeSeen);
  }
}
