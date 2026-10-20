package ro.ubm.sd.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Calculul checksum-ului SHA-256 al fisierelor, verificarea de integritate.
 */
public final class ChecksumUtil {

    private static final int BUFFER_SIZE = 64 * 1024;

    private ChecksumUtil() {
    }

    public static String sha256(Path filePath) throws IOException {
        try (InputStream in = Files.newInputStream(filePath)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Algoritmul SHA-256 nu este disponibil in JDK", e);
        }
    }

    public static String sha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Algoritmul SHA-256 nu este disponibil in JDK", e);
        }
    }
}
