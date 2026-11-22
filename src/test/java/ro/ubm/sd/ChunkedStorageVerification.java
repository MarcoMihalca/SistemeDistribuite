package ro.ubm.sd;

import ro.ubm.sd.client.Client;
import ro.ubm.sd.common.ChecksumUtil;
import ro.ubm.sd.common.ChunkMetadata;
import ro.ubm.sd.common.ChunkPolicy;
import ro.ubm.sd.common.FileMetadata;
import ro.ubm.sd.common.NodeInfo;
import ro.ubm.sd.common.NodeState;
import ro.ubm.sd.common.Protocol;
import ro.ubm.sd.node.StorageNode;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Verificare automata cap-coada pentru stocarea adaptiva pe bucati (HDFS Hybrid)
 * compatibila Java 8.
 */
public final class ChunkedStorageVerification {

    private static final int TEST_MULTICAST_PORT = 16999;
    private static final String TEST_MULTICAST_GROUP = "230.0.0.1";

    private ChunkedStorageVerification() {
    }

    public static void main(String[] args) throws Exception {
        testChunkPolicyBoundaries();
        System.out.println("[TEST] ChunkPolicy boundaries: OK");

        Path tempRoot = Files.createTempDirectory("sd-hdfs-test-");
        List<StorageNode> nodes = new ArrayList<>();

        try {
            int p1 = findFreePort();
            int p2 = findFreePort();
            int p3 = findFreePort();

            Path d1 = Files.createDirectories(tempRoot.resolve("n1"));
            Path d2 = Files.createDirectories(tempRoot.resolve("n2"));
            Path d3 = Files.createDirectories(tempRoot.resolve("n3"));

            StorageNode n1 = new StorageNode("n1", "localhost", p1, d1.toString(), true,
                    TEST_MULTICAST_GROUP, TEST_MULTICAST_PORT, false);
            StorageNode n2 = new StorageNode("n2", "localhost", p2, d2.toString(), false,
                    TEST_MULTICAST_GROUP, TEST_MULTICAST_PORT, false);
            StorageNode n3 = new StorageNode("n3", "localhost", p3, d3.toString(), false,
                    TEST_MULTICAST_GROUP, TEST_MULTICAST_PORT, false);

            nodes.add(n1);
            nodes.add(n2);
            nodes.add(n3);

            n1.start();
            n2.start();
            n3.start();

            registerPeerPair(n1, n2);
            registerPeerPair(n1, n3);
            waitForActiveNodes(n1, 3, 5000);

            Client client = new Client("localhost:" + p1);
            Path inputDir = Files.createDirectories(tempRoot.resolve("input"));
            Path outDir = Files.createDirectories(tempRoot.resolve("out"));

            testSmallFileRoundTrip(client, n1, nodes, inputDir, outDir);
            System.out.println("[TEST] Small file (200 KB) round-trip: OK");

            testEmptyFileRoundTrip(client, n1, nodes, inputDir, outDir);
            System.out.println("[TEST] Empty file (0 B) round-trip: OK");

            testMultiChunkStripingAndParallelGet(client, n1, nodes, inputDir, outDir);
            System.out.println("[TEST] Multi-chunk (7 MB) striping + parallel get + info + rm: OK");

            testDynamicNodeRebalancing(client, n1, nodes, tempRoot, inputDir);
            System.out.println("[TEST] Dynamic node (n4) rebalancing: OK");

            testDirectNodeCompatibility(client, p2, inputDir, outDir);
            System.out.println("[TEST] Direct single-node put/get compatibility: OK");

            testMidUploadFailureRollback(client, n1, nodes, inputDir);
            System.out.println("[TEST] Mid-upload failure rollback & zero orphan chunks: OK");

            System.out.println("[TEST] ALL VERIFICATION CHECKS PASSED!");
        } finally {
            for (StorageNode node : nodes) {
                try {
                    node.stop();
                } catch (Exception ignored) {
                }
            }
            deleteRecursively(tempRoot);
        }
    }

    private static void testChunkPolicyBoundaries() {
        long kb = ChunkPolicy.KB;
        long mb = ChunkPolicy.MB;
        long gb = ChunkPolicy.GB;

        assertChunkPlan(0L, 0L, 1);
        assertChunkPlan(1L, 1L, 1);
        assertChunkPlan(1L * mb, 1L * mb, 1);
        assertChunkPlan(2L * mb, 2L * mb, 1);
        assertChunkPlan(2L * mb + 1L, 2L * mb, 2);
        assertChunkPlan(5L * mb, 2L * mb, 3);
        assertChunkPlan(7L * mb, 2L * mb, 4);
        assertChunkPlan(16L * mb, 2L * mb, 8);
        assertChunkPlan(16L * mb + 1L, 16L * mb, 2);
        assertChunkPlan(100L * mb, 16L * mb, 7);
        assertChunkPlan(128L * mb, 16L * mb, 8);
        assertChunkPlan(128L * mb + 1L, 64L * mb, 3);
        assertChunkPlan(256L * mb, 64L * mb, 4);
        assertChunkPlan(1L * gb, 64L * mb, 16);
        assertChunkPlan(1L * gb + 1L, 128L * mb, 9);
        assertChunkPlan(2L * gb, 128L * mb, 16);
        assertChunkPlan(4L * gb, 128L * mb, 32);

        assertEquals(Protocol.MAX_FILE_SIZE, 4L * gb, "MAX_FILE_SIZE must be 4 GB");
        assertEquals(ChunkPolicy.chunkLengthAt(200L * kb, 200L * kb, 0), 200L * kb, "single chunk length");

        boolean threw = false;
        try {
            ChunkPolicy.chunkSizeFor(-1L);
        } catch (IllegalArgumentException expected) {
            threw = true;
        }
        assertTrue(threw, "Negative file size should throw IllegalArgumentException");
    }

    private static void assertChunkPlan(long fileSize, long expectedChunkSize, int expectedCount) {
        long actualChunkSize = ChunkPolicy.chunkSizeFor(fileSize);
        int actualCount = ChunkPolicy.chunkCountFor(fileSize);
        assertEquals(actualChunkSize, expectedChunkSize, "chunkSizeFor(" + fileSize + ")");
        assertEquals(actualCount, expectedCount, "chunkCountFor(" + fileSize + ")");

        long total = 0L;
        for (int i = 0; i < actualCount; i++) {
            long len = ChunkPolicy.chunkLengthAt(fileSize, actualChunkSize, i);
            total += len;
        }
        assertEquals(total, fileSize, "Sum of chunk lengths for size=" + fileSize);
    }

    private static void testSmallFileRoundTrip(Client client, StorageNode coordinator, List<StorageNode> nodes,
                                               Path inputDir, Path outDir) throws IOException {
        Path smallFile = createDeterministicFile(inputDir.resolve("small_200kb.bin"), 200 * 1024, 101L);
        String expectedSha = ChecksumUtil.sha256(smallFile);

        client.executeCommand(new String[]{"put", smallFile.toString()});
        FileMetadata meta = findByName(coordinator, "small_200kb.bin");
        assertTrue(meta != null, "small_200kb.bin should be committed");
        assertEquals(meta.getChunkCount(), 1, "small file should have 1 chunk");
        assertEquals(meta.getChecksum(), expectedSha, "committed SHA should match");

        Path dlDir = Files.createDirectories(outDir.resolve("small"));
        client.executeCommand(new String[]{"get", "small_200kb.bin", dlDir.toString()});
        Path downloaded = dlDir.resolve("small_200kb.bin");
        assertTrue(Files.isRegularFile(downloaded), "downloaded small file should exist");
        assertEquals(ChecksumUtil.sha256(downloaded), expectedSha, "downloaded SHA must match");
        assertTrue(!Files.exists(dlDir.resolve("small_200kb.bin.part")), "no leftover .part file");

        client.executeCommand(new String[]{"rm", "small_200kb.bin"});
        assertTrue(findByName(coordinator, "small_200kb.bin") == null, "metadata removed after rm");
        assertEquals(countAllChunkFiles(nodes), 0, "all chunk files deleted after rm");
    }

    private static void testEmptyFileRoundTrip(Client client, StorageNode coordinator, List<StorageNode> nodes,
                                               Path inputDir, Path outDir) throws IOException {
        Path emptyFile = inputDir.resolve("empty.txt");
        Files.write(emptyFile, new byte[0]);
        String expectedSha = ChecksumUtil.sha256(emptyFile);

        client.executeCommand(new String[]{"put", emptyFile.toString()});
        FileMetadata meta = findByName(coordinator, "empty.txt");
        assertTrue(meta != null, "empty.txt should be committed");
        assertEquals(meta.getSize(), 0L, "empty file size 0");
        assertEquals(meta.getChunkCount(), 1, "empty file 1 chunk");

        Path dlDir = Files.createDirectories(outDir.resolve("empty"));
        client.executeCommand(new String[]{"get", "empty.txt", dlDir.toString()});
        Path downloaded = dlDir.resolve("empty.txt");
        assertTrue(Files.isRegularFile(downloaded), "downloaded empty file should exist");
        assertEquals(Files.size(downloaded), 0L, "downloaded empty file size 0");
        assertEquals(ChecksumUtil.sha256(downloaded), expectedSha, "empty file SHA match");

        client.executeCommand(new String[]{"rm", "empty.txt"});
        assertEquals(countAllChunkFiles(nodes), 0, "all chunk files deleted for empty file");
    }

    private static void testMultiChunkStripingAndParallelGet(Client client, StorageNode coordinator,
                                                             List<StorageNode> nodes, Path inputDir,
                                                             Path outDir) throws IOException {
        long size = 7L * ChunkPolicy.MB;
        Path bigFile = createDeterministicFile(inputDir.resolve("bundle_7mb.bin"), size, 202L);
        String expectedSha = ChecksumUtil.sha256(bigFile);

        client.executeCommand(new String[]{"put", bigFile.toString()});

        FileMetadata meta = findByName(coordinator, "bundle_7mb.bin");
        assertTrue(meta != null, "bundle_7mb.bin should be committed");
        assertEquals(meta.getSize(), size, "committed file size");
        assertEquals(meta.getChunkSize(), 2L * ChunkPolicy.MB, "chunk size for 7 MB must be 2 MB");
        assertEquals(meta.getChunkCount(), 4, "7 MB must split into 4 chunks");

        Set<String> distinctNodes = new HashSet<>(meta.getReplicaNodes());
        assertEquals(distinctNodes.size(), 3, "chunks should stripe across all 3 active nodes (n1, n2, n3)");
        assertEquals(countAllChunkFiles(nodes), 4, "exact 4 chunk files on disk across cluster");

        client.executeCommand(new String[]{"ls"});
        client.executeCommand(new String[]{"info", "bundle_7mb.bin"});

        Path dlDir = Files.createDirectories(outDir.resolve("multi"));
        client.executeCommand(new String[]{"get", "bundle_7mb.bin", dlDir.toString()});
        Path downloaded = dlDir.resolve("bundle_7mb.bin");
        assertTrue(Files.isRegularFile(downloaded), "downloaded 7 MB file should exist");
        assertEquals(Files.size(downloaded), size, "downloaded 7 MB size exact");
        assertEquals(ChecksumUtil.sha256(downloaded), expectedSha, "downloaded 7 MB SHA-256 exact");
        assertTrue(!Files.exists(dlDir.resolve("bundle_7mb.bin.part")), "no leftover .part file");

        client.executeCommand(new String[]{"rm", "bundle_7mb.bin"});
        assertTrue(findByName(coordinator, "bundle_7mb.bin") == null, "metadata removed after rm");
        assertEquals(countAllChunkFiles(nodes), 0, "all 4 chunk files deleted across all nodes");
    }

    private static void testDynamicNodeRebalancing(Client client, StorageNode coordinator, List<StorageNode> nodes,
                                                   Path tempRoot, Path inputDir) throws Exception {
        Path first = createDeterministicFile(inputDir.resolve("first_5mb.bin"), 5L * ChunkPolicy.MB, 303L);
        client.executeCommand(new String[]{"put", first.toString()});
        assertTrue(findByName(coordinator, "first_5mb.bin") != null, "first_5mb.bin committed");

        int p4 = findFreePort();
        Path d4 = Files.createDirectories(tempRoot.resolve("n4"));
        StorageNode n4 = new StorageNode("n4", "localhost", p4, d4.toString(), false,
                TEST_MULTICAST_GROUP, TEST_MULTICAST_PORT, false);
        nodes.add(n4);
        n4.start();
        registerPeerPair(coordinator, n4);
        waitForActiveNodes(coordinator, 4, 5000);

        Path second = createDeterministicFile(inputDir.resolve("second_5mb.bin"), 5L * ChunkPolicy.MB, 404L);
        client.executeCommand(new String[]{"put", second.toString()});

        FileMetadata secondMeta = findByName(coordinator, "second_5mb.bin");
        assertTrue(secondMeta != null, "second_5mb.bin committed");
        boolean assignedToN4 = false;
        for (ChunkMetadata cm : secondMeta.getChunks()) {
            if (cm.getReplicaNodes().contains("n4")) {
                assignedToN4 = true;
                break;
            }
        }
        assertTrue(assignedToN4, "new empty node n4 must receive chunks immediately via Least-Bytes-Used striping");

        client.executeCommand(new String[]{"rm", "first_5mb.bin"});
        client.executeCommand(new String[]{"rm", "second_5mb.bin"});
        assertEquals(countAllChunkFiles(nodes), 0, "all chunk files cleaned after rebalancing test");
    }

    private static void testDirectNodeCompatibility(Client client, int node2Port,
                                                    Path inputDir, Path outDir) throws IOException {
        Path directFile = createDeterministicFile(inputDir.resolve("direct_128kb.bin"), 128 * 1024, 505L);
        String expectedSha = ChecksumUtil.sha256(directFile);

        client.executeCommand(new String[]{"put", "localhost:" + node2Port, directFile.toString()});
        Path dlDir = Files.createDirectories(outDir.resolve("direct"));
        client.executeCommand(new String[]{"get", "localhost:" + node2Port, "direct_128kb.bin", dlDir.toString()});

        Path downloaded = dlDir.resolve("direct_128kb.bin");
        assertTrue(Files.isRegularFile(downloaded), "direct downloaded file should exist");
        assertEquals(ChecksumUtil.sha256(downloaded), expectedSha, "direct download SHA-256 exact");
    }

    private static void testMidUploadFailureRollback(Client client, StorageNode coordinator,
                                                     List<StorageNode> nodes, Path inputDir) throws IOException {
        int deadPort = findFreePort();
        coordinator.getClusterNodes().put("n_dead",
                new NodeInfo("n_dead", "localhost", deadPort, NodeState.ACTIV, System.currentTimeMillis(), false));

        try {
            // 11 MB = 6 bucati (2 MB x 5 + 1 MB), garantand ca cel putin o bucata ajunge pe n_dead
            // in timp ce restul bucatilor de pe nodurile active trebuie sterse la rollback.
            Path failFile = createDeterministicFile(inputDir.resolve("rollback_11mb.bin"), 11L * ChunkPolicy.MB, 606L);
            client.executeCommand(new String[]{"put", failFile.toString()});

            assertTrue(findByName(coordinator, "rollback_11mb.bin") == null,
                    "failed multi-chunk upload must NOT commit metadata");
            assertEquals(countAllChunkFiles(nodes), 0,
                    "failed upload must clean up already-uploaded chunks on live nodes");
        } finally {
            coordinator.getClusterNodes().remove("n_dead");
        }
    }

    private static void registerPeerPair(StorageNode a, StorageNode b) throws IOException {
        sendTcpHeartbeat(b, a.getHost(), a.getPort());
        sendTcpHeartbeat(a, b.getHost(), b.getPort());
    }

    private static void sendTcpHeartbeat(StorageNode sender, String targetHost, int targetPort) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(targetHost, targetPort), 1500);
            OutputStream out = s.getOutputStream();
            Protocol.writeHeaderLine(out, String.format("HEARTBEAT %s %s %d %b %d",
                    sender.getId(), sender.getHost(), sender.getPort(), sender.isCoordinator(), 99999L));
            Protocol.readHeaderLine(s.getInputStream());
        }
    }

    private static void waitForActiveNodes(StorageNode coordinator, int expectedCount, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            long active = coordinator.getClusterNodes().values().stream()
                    .filter(n -> n.getState() == NodeState.ACTIV)
                    .count();
            if (active >= expectedCount) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("Timeout waiting for " + expectedCount + " active nodes on coordinator");
    }

    private static FileMetadata findByName(StorageNode coordinator, String name) {
        for (FileMetadata meta : coordinator.getFileMetadataTable().values()) {
            if (meta.getName().equals(name)) {
                return meta;
            }
        }
        return null;
    }

    private static int countAllChunkFiles(List<StorageNode> nodes) throws IOException {
        int total = 0;
        for (StorageNode node : nodes) {
            Path dir = node.getStorageDir();
            if (!Files.exists(dir)) {
                continue;
            }
            try (Stream<Path> files = Files.list(dir)) {
                List<Path> list = files.collect(java.util.stream.Collectors.toList());
                for (Path p : list) {
                    String name = p.getFileName().toString();
                    if (name.contains(".chunk.") || name.endsWith(".part")) {
                        total++;
                    }
                }
            }
        }
        return total;
    }

    private static Path createDeterministicFile(Path path, long size, long seed) throws IOException {
        Random rnd = new Random(seed);
        byte[] buf = new byte[64 * 1024];
        long remaining = size;
        try (OutputStream out = Files.newOutputStream(path)) {
            while (remaining > 0) {
                int len = (int) Math.min(buf.length, remaining);
                rnd.nextBytes(buf);
                out.write(buf, 0, len);
                remaining -= len;
            }
        }
        return path;
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void deleteRecursively(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                }
            });
        } catch (IOException ignored) {
        }
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void assertEquals(long actual, long expected, String message) {
        if (actual != expected) {
            throw new AssertionError(message + " (expected=" + expected + ", actual=" + actual + ")");
        }
    }

    private static void assertEquals(String actual, String expected, String message) {
        if (actual == null ? expected != null : !actual.equals(expected)) {
            throw new AssertionError(message + " (expected=" + expected + ", actual=" + actual + ")");
        }
    }
}
