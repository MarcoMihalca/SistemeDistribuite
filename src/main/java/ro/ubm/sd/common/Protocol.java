package ro.ubm.sd.common;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utilitare pentru incadrarea mesajelor TCP (antet UTF-8 terminat cu '\n' + continut binar)
 * si transferul in flux al fisierelor in blocuri de 64 KB (compatibil Java 8).
 */
public final class Protocol {

    public static final int BUFFER_SIZE = 64 * 1024;
    public static final long MAX_FILE_SIZE = 512L * 1024L * 1024L;
    private static final int MAX_HEADER_BYTES = 16 * 1024;

    private Protocol() {
    }

    /**
     * Citeste o linie antet UTF-8 pana la '\n' fara a pre-incarca octeti din fluxul binar urmator.
     * Returneaza null daca fluxul s-a inchis inainte de a primi vreun octet.
     */
    public static String readHeaderLine(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
        int b;
        boolean readAny = false;
        while ((b = in.read()) != -1) {
            readAny = true;
            if (b == '\n') {
                break;
            }
            if (b != '\r') {
                baos.write(b);
                if (baos.size() > MAX_HEADER_BYTES) {
                    throw new IOException("Antet TCP prea lung (>" + MAX_HEADER_BYTES + " octeti)");
                }
            }
        }
        if (!readAny) {
            return null;
        }
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * Scrie o linie antet UTF-8 terminata cu '\n' si forteaza trimiterea imediata.
     */
    public static void writeHeaderLine(OutputStream out, String header) throws IOException {
        byte[] bytes = (header + "\n").getBytes(StandardCharsets.UTF_8);
        out.write(bytes);
        out.flush();
    }

    /**
     * Citeste exact {@code size} octeti din fluxul TCP, ii scrie pe disc in {@code target}
     * si calculeaza pe drum checksum-ul SHA-256.
     */
    public static String receiveBytes(InputStream in, long size, Path target) throws IOException {
        MessageDigest sha = newSha256Digest();
        byte[] buffer = new byte[BUFFER_SIZE];
        long remaining = size;

        try (OutputStream out = Files.newOutputStream(target)) {
            while (remaining > 0) {
                int toRead = (int) Math.min(buffer.length, remaining);
                int n = in.read(buffer, 0, toRead);
                if (n < 0) {
                    throw new EOFException("Conexiune inchisa dupa " + (size - remaining)
                            + " din " + size + " octeti");
                }
                out.write(buffer, 0, n);
                sha.update(buffer, 0, n);
                remaining -= n;
            }
        }
        return ChecksumUtil.toHex(sha.digest());
    }

    /**
     * Trimite continutul unui fisier pe fluxul TCP in blocuri de 64 KB.
     */
    public static void sendFileBytes(Path source, OutputStream out) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        try (InputStream in = Files.newInputStream(source)) {
            int n;
            while ((n = in.read(buffer)) != -1) {
                out.write(buffer, 0, n);
            }
        }
        out.flush();
    }

    /**
     * Muta un fisier temporar (.part) la destinatia finala printr-o operatie atomica
     * (cu fallback la inlocuire simpla daca sistemul de fisiere nu suporta ATOMIC_MOVE).
     */
    public static void moveAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Valideaza ca numele fisierului nu contine separatori de cale, ".." sau spatii albe.
     */
    public static String sanitizeFileName(String rawName) {
        if (rawName == null) {
            throw new IllegalArgumentException("Numele fisierului lipseste");
        }
        String trimmed = rawName.trim();
        if (trimmed.isEmpty() || ".".equals(trimmed) || "..".equals(trimmed)
                || trimmed.contains("/") || trimmed.contains("\\")) {
            throw new IllegalArgumentException("Nume de fisier invalid: " + rawName);
        }
        for (int i = 0; i < trimmed.length(); i++) {
            if (Character.isWhitespace(trimmed.charAt(i))) {
                throw new IllegalArgumentException("Numele fisierului nu poate contine spatii: " + rawName);
            }
        }
        Path fileName = Paths.get(trimmed).getFileName();
        if (fileName == null || !trimmed.equals(fileName.toString())) {
            throw new IllegalArgumentException("Nume de fisier invalid: " + rawName);
        }
        return trimmed;
    }

    private static MessageDigest newSha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Algoritmul SHA-256 nu este disponibil in JDK", e);
        }
    }
}
