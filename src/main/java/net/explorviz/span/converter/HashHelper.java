package net.explorviz.span.converter;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class HashHelper {
    private static final long[] HIGHWAY_HASH_KEY = {0x45_78_70_6c_6f_72_56_69L, 0x7a_53_70_61_6e_73_48_69L,
        0x67_68_77_61_79_48_61_73L, 0x68_43_6f_64_65_4b_65_79L};

    private HashHelper() {
    }

    public static long calculateSpanHash(final UUID landscapeToken, final String nodeIpAddress,
                                         final String applicationName, final int applicationInstance,
                                         final String methodFqn) {
        HighwayHash hash = new HighwayHash(HIGHWAY_HASH_KEY);

        // TODO: Fill with IPv6 address bits (Convert IPv4 to IPv4-in-IPv6 representation)
        hash.update(
            landscapeToken.getMostSignificantBits(), landscapeToken.getLeastSignificantBits(), applicationInstance, 0L
        );

        String builder = applicationName + ';' + nodeIpAddress + ';' + methodFqn;
        byte[] bytes = builder.getBytes(StandardCharsets.UTF_8);
        int position = 0;
        for (; bytes.length - position >= 32; position += 32) {
            hash.updatePacket(bytes, position);
        }
        int remaining = bytes.length - position;
        if (remaining > 0) {
            hash.updateRemainder(bytes, position, remaining);
        }

        return hash.finalize64();
    }
}
