package ro.ubm.sd.common;

import java.util.Locale;
import java.util.UUID;

/**
 * Politica adaptiva de impartire a fisierelor in bucati (HDFS hybrid).
 * Pragurile sunt puteri ale lui 2 aliniate cu buffer-ul TCP de 64 KB.
 */
public final class ChunkPolicy {

    public static final long KB = 1024L;
    public static final long MB = 1024L * 1024L;
    public static final long GB = 1024L * 1024L * 1024L;

    public static final long SMALL_FILE_THRESHOLD = 2L * MB;
    public static final long TIER_1_MAX = 16L * MB;
    public static final long TIER_2_MAX = 128L * MB;
    public static final long TIER_3_MAX = 1L * GB;

    public static final long TIER_1_CHUNK_SIZE = 2L * MB;
    public static final long TIER_2_CHUNK_SIZE = 16L * MB;
    public static final long TIER_3_CHUNK_SIZE = 64L * MB;
    public static final long TIER_4_CHUNK_SIZE = 128L * MB;

    private ChunkPolicy() {
    }

    /**
     * Returneaza dimensiunea optima a unei bucati pentru un fisier cu dimensiunea data.
     * Fisierele <= 2 MB nu sunt impartite.
     */
    public static long chunkSizeFor(long fileSize) {
        if (fileSize < 0) {
            throw new IllegalArgumentException("Dimensiunea fisierului nu poate fi negativa: " + fileSize);
        }
        if (fileSize == 0L) {
            return 0L;
        }
        if (fileSize <= SMALL_FILE_THRESHOLD) {
            return fileSize;
        }
        if (fileSize <= TIER_1_MAX) {
            return TIER_1_CHUNK_SIZE;
        }
        if (fileSize <= TIER_2_MAX) {
            return TIER_2_CHUNK_SIZE;
        }
        if (fileSize <= TIER_3_MAX) {
            return TIER_3_CHUNK_SIZE;
        }
        return TIER_4_CHUNK_SIZE;
    }

    /**
     * Returneaza numarul total de bucati rezultate pentru dimensiunea data.
     */
    public static int chunkCountFor(long fileSize) {
        if (fileSize < 0) {
            throw new IllegalArgumentException("Dimensiunea fisierului nu poate fi negativa: " + fileSize);
        }
        if (fileSize <= SMALL_FILE_THRESHOLD) {
            return 1;
        }
        long cs = chunkSizeFor(fileSize);
        long count = (fileSize + cs - 1L) / cs;
        if (count > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Prea multe bucati pentru fisierul de " + fileSize + " octeti");
        }
        return (int) count;
    }

    /**
     * Returneaza dimensiunea exacta in octeti a bucatii cu indexul dat.
     */
    public static long chunkLengthAt(long fileSize, long chunkSize, int index) {
        if (fileSize < 0 || index < 0) {
            throw new IllegalArgumentException("Parametri invalizi: fileSize=" + fileSize + ", index=" + index);
        }
        if (fileSize == 0L) {
            if (index == 0) {
                return 0L;
            }
            throw new IllegalArgumentException("Index invalid pentru fisier gol: " + index);
        }
        if (chunkSize <= 0L) {
            throw new IllegalArgumentException("Dimensiunea bucatii trebuie sa fie pozitiva: " + chunkSize);
        }
        long offset = (long) index * chunkSize;
        if (offset >= fileSize) {
            throw new IllegalArgumentException("Index de bucata in afara fisierului: " + index);
        }
        return Math.min(chunkSize, fileSize - offset);
    }

    /**
     * Construieste numele unic al bucatii pe disc: {@code <fileUUID>.chunk.<index>}.
     */
    public static String chunkFileName(UUID fileId, int index) {
        if (fileId == null) {
            throw new IllegalArgumentException("fileId nu poate fi null");
        }
        if (index < 0) {
            throw new IllegalArgumentException("index nu poate fi negativ: " + index);
        }
        return fileId.toString() + ".chunk." + index;
    }

    /**
     * Formateaza o dimensiune in octeti intr-o reprezentare lizibila (B, KB, MB, GB).
     */
    public static String formatHumanSize(long bytes) {
        if (bytes < KB) {
            return bytes + " B";
        }
        if (bytes < MB) {
            return formatUnit(bytes, KB, "KB");
        }
        if (bytes < GB) {
            return formatUnit(bytes, MB, "MB");
        }
        return formatUnit(bytes, GB, "GB");
    }

    private static String formatUnit(long bytes, long unit, String label) {
        if (bytes % unit == 0L) {
            return (bytes / unit) + " " + label;
        }
        return String.format(Locale.ROOT, "%.1f %s", bytes / (double) unit, label);
    }
}
