package org.macaroon3145.folia.bridge;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.LongStream;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.network.protocol.game.ClientboundLevelChunkPacketData;
import net.minecraft.server.level.ThreadedLevelLightEngine;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.LightLayer;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.chunk.DataLayer;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.LevelChunkSection;
import net.minecraft.world.level.lighting.LevelLightEngine;
import net.minecraft.world.level.levelgen.Heightmap;
import net.minecraft.world.level.chunk.PalettedContainerRO;
import net.minecraft.world.level.chunk.Strategy;
import org.bukkit.Bukkit;
import org.bukkit.World;
import org.bukkit.WorldCreator;
import org.bukkit.craftbukkit.CraftWorld;
import org.bukkit.plugin.java.JavaPlugin;

public final class AerogelFoliaBridgePlugin extends JavaPlugin {
    private static final String DEFAULT_WORLD_NAME = "world";
    private static final String WORLD_NETHER_NAME = "world_nether";
    private static final String WORLD_END_NAME = "world_the_end";
    private static final long METRICS_LOG_INTERVAL_NANOS = 1_000_000_000L;
    private static final String OVERWORLD_SEED_PROPERTY = "aerogel.folia.seed.overworld";
    private static final String NETHER_SEED_PROPERTY = "aerogel.folia.seed.the_nether";
    private static final String END_SEED_PROPERTY = "aerogel.folia.seed.the_end";
    private static final String WORLD_SPAWNS_FILE = "world-spawns.tsv";
    private static final long CHUNK_RETENTION_SWEEP_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1L);
    private static final long DEFAULT_CHUNK_KEEPALIVE_NANOS = TimeUnit.SECONDS.toNanos(30L);

    private SharedMemoryBridge bridge;
    private volatile boolean pollerRunning;
    private Thread pollerThread;
    private Path bridgeReadyMarker;
    private World overworld;
    private World nether;
    private World end;
    private World fallbackWorld;
    private ExecutorService responseExecutor;
    private volatile int bridgeSlotCount;
    private final AtomicInteger responseWorkerId = new AtomicInteger(1);
    private final AtomicLong claimedTotal = new AtomicLong();
    private final AtomicLong completedTotal = new AtomicLong();
    private final AtomicLong failedTotal = new AtomicLong();
    private final AtomicLong generationStageNanosTotal = new AtomicLong();
    private final AtomicLong extractionStageNanosTotal = new AtomicLong();
    private final AtomicLong responseStageNanosTotal = new AtomicLong();
    private final AtomicLong timedCompletedTotal = new AtomicLong();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxInFlight = new AtomicInteger();
    private final AtomicInteger startupWaveClaims = new AtomicInteger();
    private final AtomicBoolean startupWaveDone = new AtomicBoolean(false);
    private volatile long startupWaveStartNanos = -1L;
    private volatile long lastMetricsLogNanos;
    private volatile long lastClaimedSnapshot;
    private volatile long lastCompletedSnapshot;
    private volatile long lastFailedSnapshot;
    private volatile long lastGenerationStageNanosSnapshot;
    private volatile long lastExtractionStageNanosSnapshot;
    private volatile long lastResponseStageNanosSnapshot;
    private volatile long lastTimedCompletedSnapshot;
    private volatile long lastChunkRetentionSweepNanos;
    private final long chunkKeepAliveNanos = configuredChunkKeepAliveNanos();
    private final Map<CachedChunkKey, Long> retainedChunks = new java.util.concurrent.ConcurrentHashMap<>();
    private final Long configuredOverworldSeed = configuredWorldSeed(OVERWORLD_SEED_PROPERTY);
    private final Long configuredNetherSeed = configuredWorldSeed(NETHER_SEED_PROPERTY);
    private final Long configuredEndSeed = configuredWorldSeed(END_SEED_PROPERTY);

    @Override
    public void onEnable() {
        try {
            int slotCount = configuredSlotCount();
            resetMetrics(slotCount);
            Path runtimeDir = resolveRuntimeDir();
            this.bridge = SharedMemoryBridge.open(runtimeDir, slotCount);
            this.responseExecutor = Executors.newFixedThreadPool(configuredResponseWorkerCount(slotCount), runnable ->
                new Thread(runnable, "aerogel-folia-bridge-response-" + this.responseWorkerId.getAndIncrement())
            );
            disableWorldPersistence();
            cacheWorldRefs();
            writeWorldSpawnSnapshot(runtimeDir);
            this.bridgeReadyMarker = writeReadyMarker(runtimeDir, slotCount);
            startPollerThread();
            getLogger().info("Aerogel Folia bridge enabled (shared memory slots=" + slotCount + ")");
        } catch (Exception exception) {
            clearReadyMarker();
            if (this.bridge != null) {
                this.bridge.close();
                this.bridge = null;
            }
            getLogger().severe("Failed to initialize chunk shared-memory bridge: " + exception.getMessage());
            getServer().getPluginManager().disablePlugin(this);
        }
    }

    @Override
    public void onDisable() {
        stopPollerThread();

        ExecutorService executor = this.responseExecutor;
        this.responseExecutor = null;
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(2L, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        }

        if (this.bridge != null) {
            this.bridge.close();
            this.bridge = null;
        }
        clearReadyMarker();
        getLogger().info("Aerogel Folia bridge plugin disabled");
    }

    private void startPollerThread() {
        this.pollerRunning = true;
        Thread thread = new Thread(this::pollLoop, "aerogel-folia-bridge-poller");
        thread.setDaemon(true);
        this.pollerThread = thread;
        thread.start();
    }

    private void stopPollerThread() {
        this.pollerRunning = false;
        Thread thread = this.pollerThread;
        if (thread == null) return;
        thread.interrupt();
        try {
            thread.join(2000L);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        this.pollerThread = null;
    }

    private void pollLoop() {
        while (this.pollerRunning && !Thread.currentThread().isInterrupted()) {
            boolean claimed = pollRequests();
            sweepRetainedChunksIfDue();
            logMetricsIfDue();
            if (!claimed) {
                LockSupport.parkNanos(200_000L);
            }
        }
    }

    private boolean pollRequests() {
        if (this.bridge == null) return false;
        boolean claimedAny = false;

        int slotCount = this.bridge.slotCount();
        for (int slotIndex = 0; slotIndex < slotCount; slotIndex++) {
            SharedMemoryBridge.ChunkRequest request = this.bridge.tryClaimRequest(slotIndex);
            if (request == null) continue;
            claimedAny = true;
            onRequestClaimed(request);

            final World world = resolveWorld(request.worldKey());
            if (world == null) {
                this.bridge.completeError(request, "World not found: " + request.worldKey());
                markRequestFinished(false);
                continue;
            }

            Bukkit.getRegionScheduler().execute(this, world, request.chunkX(), request.chunkZ(), () -> handleRequest(world, request));
        }
        return claimedAny;
    }

    private void disableWorldPersistence() {
        // Disable periodic saves to avoid duplicate persistence (Aerogel persists independently).
        for (World world : Bukkit.getWorlds()) {
            world.setAutoSave(false);
        }
    }

    private void cacheWorldRefs() {
        this.overworld = ensureSeededWorld(DEFAULT_WORLD_NAME, World.Environment.NORMAL, this.configuredOverworldSeed);
        this.nether = ensureSeededWorld(WORLD_NETHER_NAME, World.Environment.NETHER, this.configuredNetherSeed);
        this.end = ensureSeededWorld(WORLD_END_NAME, World.Environment.THE_END, this.configuredEndSeed);
        this.fallbackWorld = this.overworld != null
            ? this.overworld
            : (!Bukkit.getWorlds().isEmpty() ? Bukkit.getWorlds().get(0) : null);
    }

    private Long configuredWorldSeed(String propertyKey) {
        String raw = System.getProperty(propertyKey);
        if (raw == null || raw.isBlank()) return null;
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private World ensureSeededWorld(String worldName, World.Environment environment, Long targetSeed) {
        World existing = Bukkit.getWorld(worldName);
        if (targetSeed == null) {
            if (existing != null) {
                existing.setAutoSave(false);
                return existing;
            }
            World created = createSeededWorld(worldName, environment, null);
            if (created != null) created.setAutoSave(false);
            return created;
        }

        if (existing != null) {
            if (existing.getSeed() == targetSeed) {
                existing.setAutoSave(false);
                return existing;
            }
            getLogger().warning(
                "World seed mismatch for '" + worldName + "' (configured=" + targetSeed + ", loaded=" + existing.getSeed()
                    + "). Keeping loaded world."
            );
            existing.setAutoSave(false);
            return existing;
        }

        World created = createSeededWorld(worldName, environment, targetSeed);
        if (created != null) {
            created.setAutoSave(false);
            return created;
        }

        World fallback = Bukkit.getWorld(worldName);
        if (fallback != null) fallback.setAutoSave(false);
        return fallback;
    }

    private World createSeededWorld(String worldName, World.Environment environment, Long seed) {
        WorldCreator creator = new WorldCreator(worldName);
        creator.environment(environment);
        if (seed != null) {
            creator.seed(seed);
        }
        return Bukkit.createWorld(creator);
    }

    private Path writeReadyMarker(Path runtimeDir, int slotCount) throws IOException {
        Path marker = runtimeDir.resolve("ipc").resolve("bridge-ready");
        Files.createDirectories(marker.getParent());
        Files.writeString(
            marker,
            "slots=" + slotCount + "\n",
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        );
        return marker;
    }

    private void writeWorldSpawnSnapshot(Path runtimeDir) throws IOException {
        Path snapshot = runtimeDir.resolve("ipc").resolve(WORLD_SPAWNS_FILE);
        Files.createDirectories(snapshot.getParent());

        StringBuilder content = new StringBuilder(192);
        appendWorldSpawn(content, "minecraft:overworld", this.overworld);
        appendWorldSpawn(content, "minecraft:the_nether", this.nether);
        appendWorldSpawn(content, "minecraft:the_end", this.end);

        Files.writeString(
            snapshot,
            content.toString(),
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        );
    }

    private void appendWorldSpawn(StringBuilder output, String worldKey, World world) {
        if (world == null) return;
        org.bukkit.Location spawn = world.getSpawnLocation();
        output.append(worldKey)
            .append('\t')
            .append(spawn.getX())
            .append('\t')
            .append(spawn.getY())
            .append('\t')
            .append(spawn.getZ())
            .append('\n');
    }

    private void clearReadyMarker() {
        Path marker = this.bridgeReadyMarker;
        if (marker == null) return;
        try {
            Files.deleteIfExists(marker);
        } catch (IOException ignored) {
        }
        this.bridgeReadyMarker = null;
    }

    private void handleRequest(World world, SharedMemoryBridge.ChunkRequest request) {
        int inflightNow = this.inFlight.incrementAndGet();
        updateMaxInFlight(inflightNow);
        long generationStageStart = System.nanoTime();
        world.getChunkAtAsync(request.chunkX(), request.chunkZ(), true, true).whenComplete((ignored, generationError) -> {
            long afterGeneration = System.nanoTime();
            this.generationStageNanosTotal.addAndGet(Math.max(0L, afterGeneration - generationStageStart));
            if (generationError != null) {
                completeRequestError(world, request, generationError);
                markRequestFinished(false);
                return;
            }

            SharedMemoryBridge bridgeSnapshot = this.bridge;
            if (bridgeSnapshot == null || !bridgeSnapshot.isRequestActive(request)) {
                retainChunk(request.worldKey(), request.chunkX(), request.chunkZ());
                markRequestFinished(false);
                return;
            }

            try {
                long extractionStageStart = System.nanoTime();
                ChunkPayload payload = extractChunkPayload(world, request.chunkX(), request.chunkZ());
                long extractionDone = System.nanoTime();
                this.extractionStageNanosTotal.addAndGet(Math.max(0L, extractionDone - extractionStageStart));
                retainChunk(request.worldKey(), request.chunkX(), request.chunkZ());

                ExecutorService executor = this.responseExecutor;
                if (executor == null || executor.isShutdown()) {
                    completeRequestError(world, request, new IllegalStateException("Bridge response executor unavailable"));
                    markRequestFinished(false);
                    return;
                }

                executor.execute(() -> {
                    long responseStageStart = System.nanoTime();
                    try {
                        SharedMemoryBridge localBridge = this.bridge;
                        if (localBridge == null) {
                            throw new IllegalStateException("Bridge closed");
                        }
                        if (!localBridge.isRequestActive(request)) {
                            markRequestFinished(false);
                            return;
                        }
                        localBridge.completeSuccess(request, payload);
                        long responseDone = System.nanoTime();
                        this.responseStageNanosTotal.addAndGet(Math.max(0L, responseDone - responseStageStart));
                        this.timedCompletedTotal.incrementAndGet();
                        markRequestFinished(true);
                    } catch (Throwable throwable) {
                        completeRequestError(world, request, throwable);
                        markRequestFinished(false);
                    }
                });
            } catch (Throwable throwable) {
                retainChunk(request.worldKey(), request.chunkX(), request.chunkZ());
                completeRequestError(world, request, throwable);
                markRequestFinished(false);
            }
        });
    }

    private void scheduleChunkUnload(World world, int chunkX, int chunkZ) {
        Bukkit.getRegionScheduler().execute(this, world, chunkX, chunkZ, () -> {
            boolean unloaded = world.unloadChunkRequest(chunkX, chunkZ);
            if (!unloaded) {
                world.unloadChunk(chunkX, chunkZ, false);
            }
        });
    }

    private long configuredChunkKeepAliveNanos() {
        String configured = System.getProperty("aerogel.folia.bridge.chunk-keepalive-ms");
        long keepAliveMillis;
        if (configured == null || configured.isBlank()) {
            keepAliveMillis = TimeUnit.NANOSECONDS.toMillis(DEFAULT_CHUNK_KEEPALIVE_NANOS);
        } else {
            try {
                keepAliveMillis = Math.max(1L, Long.parseLong(configured.trim()));
            } catch (NumberFormatException ignored) {
                keepAliveMillis = TimeUnit.NANOSECONDS.toMillis(DEFAULT_CHUNK_KEEPALIVE_NANOS);
            }
        }
        return TimeUnit.MILLISECONDS.toNanos(keepAliveMillis);
    }

    private void retainChunk(String worldKey, int chunkX, int chunkZ) {
        this.retainedChunks.put(new CachedChunkKey(worldKey, chunkX, chunkZ), System.nanoTime());
    }

    private void sweepRetainedChunksIfDue() {
        long now = System.nanoTime();
        long lastSweep = this.lastChunkRetentionSweepNanos;
        if (lastSweep != 0L && now - lastSweep < CHUNK_RETENTION_SWEEP_INTERVAL_NANOS) return;
        this.lastChunkRetentionSweepNanos = now;

        for (Map.Entry<CachedChunkKey, Long> entry : this.retainedChunks.entrySet()) {
            Long touchedAt = entry.getValue();
            if (touchedAt == null) continue;
            if (now - touchedAt < this.chunkKeepAliveNanos) continue;

            CachedChunkKey key = entry.getKey();
            if (!this.retainedChunks.remove(key, touchedAt)) continue;
            World world = resolveWorld(key.worldKey());
            if (world == null) continue;
            scheduleChunkUnload(world, key.chunkX(), key.chunkZ());
        }
    }

    private void completeRequestError(World world, SharedMemoryBridge.ChunkRequest request, Throwable throwable) {
        String message = throwable.getClass().getSimpleName() + ": " + (throwable.getMessage() == null ? "unknown" : throwable.getMessage());
        SharedMemoryBridge localBridge = this.bridge;
        if (localBridge != null) {
            localBridge.completeError(request, message);
        }
        getLogger().warning(
            "Chunk bridge failed slot=" + request.slotIndex()
                + " world=" + world.getName()
                + " key=" + request.worldKey()
                + " chunk=(" + request.chunkX() + "," + request.chunkZ() + "): " + message
        );
    }

    private int configuredResponseWorkerCount(int slotCount) {
        return Math.max(2, Math.min(slotCount, 8));
    }

    private void resetMetrics(int slotCount) {
        this.bridgeSlotCount = Math.max(1, slotCount);
        this.claimedTotal.set(0L);
        this.completedTotal.set(0L);
        this.failedTotal.set(0L);
        this.generationStageNanosTotal.set(0L);
        this.extractionStageNanosTotal.set(0L);
        this.responseStageNanosTotal.set(0L);
        this.timedCompletedTotal.set(0L);
        this.inFlight.set(0);
        this.maxInFlight.set(0);
        this.startupWaveClaims.set(0);
        this.startupWaveDone.set(false);
        this.startupWaveStartNanos = -1L;
        this.lastMetricsLogNanos = System.nanoTime();
        this.lastClaimedSnapshot = 0L;
        this.lastCompletedSnapshot = 0L;
        this.lastFailedSnapshot = 0L;
        this.lastGenerationStageNanosSnapshot = 0L;
        this.lastExtractionStageNanosSnapshot = 0L;
        this.lastResponseStageNanosSnapshot = 0L;
        this.lastTimedCompletedSnapshot = 0L;
        this.lastChunkRetentionSweepNanos = 0L;
        this.retainedChunks.clear();
    }

    private void onRequestClaimed(SharedMemoryBridge.ChunkRequest request) {
        long now = System.nanoTime();
        this.claimedTotal.incrementAndGet();
        if (this.startupWaveStartNanos < 0L) {
            this.startupWaveStartNanos = now;
        }
        if (!this.startupWaveDone.get()) {
            int waveCount = this.startupWaveClaims.incrementAndGet();
            long ageMs = (now - this.startupWaveStartNanos) / 1_000_000L;
            if (waveCount <= this.bridgeSlotCount) {
                getLogger().info(
                    "Bridge startup wave " + waveCount + "/" + this.bridgeSlotCount
                        + " claimed slot=" + request.slotIndex()
                        + " reqId=" + request.requestId()
                        + " ageMs=" + ageMs
                );
            }
            if (waveCount >= this.bridgeSlotCount && this.startupWaveDone.compareAndSet(false, true)) {
                getLogger().info(
                    "Bridge startup wave reached " + this.bridgeSlotCount
                        + " concurrent claims in " + ageMs + "ms"
                );
            }
        }
    }

    private void markRequestFinished(boolean success) {
        this.inFlight.updateAndGet(value -> value > 0 ? value - 1 : 0);
        if (success) {
            this.completedTotal.incrementAndGet();
        } else {
            this.failedTotal.incrementAndGet();
        }
    }

    private void updateMaxInFlight(int observed) {
        while (true) {
            int currentMax = this.maxInFlight.get();
            if (observed <= currentMax) return;
            if (this.maxInFlight.compareAndSet(currentMax, observed)) return;
        }
    }

    private void logMetricsIfDue() {
        long now = System.nanoTime();
        if (now - this.lastMetricsLogNanos < METRICS_LOG_INTERVAL_NANOS) return;
        this.lastMetricsLogNanos = now;

        long claimed = this.claimedTotal.get();
        long completed = this.completedTotal.get();
        long failed = this.failedTotal.get();
        long generationNanos = this.generationStageNanosTotal.get();
        long extractionNanos = this.extractionStageNanosTotal.get();
        long responseNanos = this.responseStageNanosTotal.get();
        long timedCompleted = this.timedCompletedTotal.get();

        long claimedDelta = claimed - this.lastClaimedSnapshot;
        long completedDelta = completed - this.lastCompletedSnapshot;
        long failedDelta = failed - this.lastFailedSnapshot;
        long generationNanosDelta = generationNanos - this.lastGenerationStageNanosSnapshot;
        long extractionNanosDelta = extractionNanos - this.lastExtractionStageNanosSnapshot;
        long responseNanosDelta = responseNanos - this.lastResponseStageNanosSnapshot;
        long timedCompletedDelta = timedCompleted - this.lastTimedCompletedSnapshot;

        this.lastClaimedSnapshot = claimed;
        this.lastCompletedSnapshot = completed;
        this.lastFailedSnapshot = failed;
        this.lastGenerationStageNanosSnapshot = generationNanos;
        this.lastExtractionStageNanosSnapshot = extractionNanos;
        this.lastResponseStageNanosSnapshot = responseNanos;
        this.lastTimedCompletedSnapshot = timedCompleted;

        int currentInFlight = this.inFlight.get();
        if (claimedDelta == 0L && completedDelta == 0L && failedDelta == 0L && currentInFlight == 0) {
            return;
        }
        double avgGenerationMs = timedCompletedDelta > 0L
            ? ((double) generationNanosDelta / (double) timedCompletedDelta / 1_000_000.0)
            : 0.0;
        double avgExtractionMs = timedCompletedDelta > 0L
            ? ((double) extractionNanosDelta / (double) timedCompletedDelta / 1_000_000.0)
            : 0.0;
        double avgResponseMs = timedCompletedDelta > 0L
            ? ((double) responseNanosDelta / (double) timedCompletedDelta / 1_000_000.0)
            : 0.0;
        getLogger().info(
            "Bridge metrics 1s: claimed=" + claimedDelta
                + ", completed=" + completedDelta
                + ", failed=" + failedDelta
                + ", inFlight=" + currentInFlight
                + ", maxInFlight=" + this.maxInFlight.get()
                + ", avgGenerationMs=" + String.format("%.2f", avgGenerationMs)
                + ", avgExtractionMs=" + String.format("%.2f", avgExtractionMs)
                + ", avgResponseMs=" + String.format("%.2f", avgResponseMs)
                + ", totals(c=" + claimed + ", ok=" + completed + ", err=" + failed + ")"
        );
    }

    private ChunkPayload extractChunkPayload(World world, int chunkX, int chunkZ) {
        ServerLevel level = ((CraftWorld) world).getHandle();
        LevelChunk levelChunk = level.getChunk(chunkX, chunkZ);

        ClientboundLevelChunkPacketData chunkData = new ClientboundLevelChunkPacketData(levelChunk);
        FriendlyByteBuf chunkBuffer = chunkData.getReadBuffer();
        byte[] chunkBytes = new byte[chunkBuffer.readableBytes()];
        chunkBuffer.readBytes(chunkBytes);

        List<HeightmapPayload> heightmaps = new ArrayList<>();
        for (Map.Entry<Heightmap.Types, long[]> entry : chunkData.getHeightmaps().entrySet()) {
            int typeId = entry.getKey().ordinal();
            long[] values = entry.getValue();
            heightmaps.add(new HeightmapPayload(typeId, Arrays.copyOf(values, values.length)));
        }

        LightPayload light = collectLightPayload(level, chunkX, chunkZ);
        return new ChunkPayload(
            heightmaps,
            chunkBytes,
            collectSnapshotSections(levelChunk),
            light.skyLightMask(),
            light.blockLightMask(),
            light.emptySkyLightMask(),
            light.emptyBlockLightMask(),
            light.skyLight(),
            light.blockLight()
        );
    }

    private List<SnapshotSectionPayload> collectSnapshotSections(LevelChunk levelChunk) {
        LevelChunkSection[] sections = levelChunk.getSections();
        List<SnapshotSectionPayload> out = new ArrayList<>(sections.length);
        Strategy<BlockState> strategy = Strategy.createForBlockStates(Block.BLOCK_STATE_REGISTRY);
        for (LevelChunkSection section : sections) {
            PalettedContainerRO.PackedData<BlockState> packed = section.getStates().pack(strategy);
            List<BlockState> paletteEntries = packed.paletteEntries();
            int[] palette = new int[paletteEntries.size()];
            for (int i = 0; i < paletteEntries.size(); i++) {
                palette[i] = Block.getId(paletteEntries.get(i));
            }
            long[] storage = packed.storage().orElseGet(LongStream::empty).toArray();
            out.add(new SnapshotSectionPayload(packed.bitsPerEntry(), palette, storage));
        }
        return out;
    }

    private LightPayload collectLightPayload(ServerLevel level, int chunkX, int chunkZ) {
        scheduleLightEngineUpdate(level);

        LevelLightEngine lightEngine = level.getChunkSource().getLightEngine();
        ChunkPos chunkPos = new ChunkPos(chunkX, chunkZ);

        BitSet skyMask = new BitSet();
        BitSet blockMask = new BitSet();
        BitSet emptySkyMask = new BitSet();
        BitSet emptyBlockMask = new BitSet();
        List<byte[]> skyUpdates = new ArrayList<>();
        List<byte[]> blockUpdates = new ArrayList<>();

        int lightSectionCount = lightEngine.getLightSectionCount();
        int minLightSection = lightEngine.getMinLightSection();

        for (int sectionIndex = 0; sectionIndex < lightSectionCount; sectionIndex++) {
            int sectionY = minLightSection + sectionIndex;
            net.minecraft.core.SectionPos sectionPos = net.minecraft.core.SectionPos.of(chunkPos, sectionY);

            DataLayer skyLayer = lightEngine.getLayerListener(LightLayer.SKY).getDataLayerData(sectionPos);
            if (skyLayer != null) {
                if (skyLayer.isEmpty()) {
                    emptySkyMask.set(sectionIndex);
                } else {
                    skyMask.set(sectionIndex);
                    skyUpdates.add(Arrays.copyOf(skyLayer.getData(), skyLayer.getData().length));
                }
            }

            DataLayer blockLayer = lightEngine.getLayerListener(LightLayer.BLOCK).getDataLayerData(sectionPos);
            if (blockLayer != null) {
                if (blockLayer.isEmpty()) {
                    emptyBlockMask.set(sectionIndex);
                } else {
                    blockMask.set(sectionIndex);
                    blockUpdates.add(Arrays.copyOf(blockLayer.getData(), blockLayer.getData().length));
                }
            }
        }

        return new LightPayload(
            skyMask.toLongArray(),
            blockMask.toLongArray(),
            emptySkyMask.toLongArray(),
            emptyBlockMask.toLongArray(),
            skyUpdates,
            blockUpdates
        );
    }

    private void scheduleLightEngineUpdate(ServerLevel level) {
        LevelLightEngine lightEngine = level.getChunkSource().getLightEngine();
        if (!(lightEngine instanceof ThreadedLevelLightEngine threaded)) {
            return;
        }
        threaded.tryScheduleUpdate();
    }

    private World resolveWorld(String worldKey) {
        if (worldKey == null || worldKey.isBlank()) {
            return this.overworld != null ? this.overworld : this.fallbackWorld;
        }

        switch (worldKey) {
            case "minecraft:overworld":
                return this.overworld != null ? this.overworld : this.fallbackWorld;
            case "minecraft:the_nether":
                return this.nether != null ? this.nether : this.fallbackWorld;
            case "minecraft:the_end":
                return this.end != null ? this.end : this.fallbackWorld;
            default:
                return this.fallbackWorld;
        }
    }

    private int configuredSlotCount() {
        String raw = System.getProperty("aerogel.chunk.ipc.slots");
        int parsed = 0;
        if (raw != null) {
            try {
                parsed = Integer.parseInt(raw.trim());
            } catch (NumberFormatException ignored) {
                parsed = 0;
            }
        }
        if (parsed > 0) return parsed;
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }

    private Path resolveRuntimeDir() {
        String configured = System.getProperty("aerogel.runtime.dir");
        if (configured != null && !configured.isBlank()) {
            return Path.of(configured).toAbsolutePath().normalize();
        }
        return Path.of(".").toAbsolutePath().normalize();
    }

    private record HeightmapPayload(int typeId, long[] values) {
    }

    private record ChunkPayload(
        List<HeightmapPayload> heightmaps,
        byte[] chunkData,
        List<SnapshotSectionPayload> snapshotSections,
        long[] skyLightMask,
        long[] blockLightMask,
        long[] emptySkyLightMask,
        long[] emptyBlockLightMask,
        List<byte[]> skyLight,
        List<byte[]> blockLight
    ) {
    }

    private record SnapshotSectionPayload(
        int bitsPerEntry,
        int[] palette,
        long[] storage
    ) {
    }

    private record LightPayload(
        long[] skyLightMask,
        long[] blockLightMask,
        long[] emptySkyLightMask,
        long[] emptyBlockLightMask,
        List<byte[]> skyLight,
        List<byte[]> blockLight
    ) {
    }

    private record CachedChunkKey(
        String worldKey,
        int chunkX,
        int chunkZ
    ) {
    }

    private static final class SharedMemoryBridge {
        private static final int REQUEST_MAGIC = 0x41525131; // ARQ1
        private static final int RESPONSE_MAGIC = 0x41525331; // ARS1

        private static final int REQUEST_FILE_SIZE = 4096;
        private static final int RESPONSE_FILE_SIZE = 8 * 1024 * 1024;

        private static final int REQUEST_STATE_OFFSET = 4;
        private static final int REQUEST_ID_OFFSET = 8;
        private static final int REQUEST_WORLD_LEN_OFFSET = 16;
        private static final int REQUEST_WORLD_OFFSET = 20;
        private static final int REQUEST_WORLD_MAX_BYTES = 256;
        private static final int REQUEST_CHUNK_X_OFFSET = REQUEST_WORLD_OFFSET + REQUEST_WORLD_MAX_BYTES;
        private static final int REQUEST_CHUNK_Z_OFFSET = REQUEST_CHUNK_X_OFFSET + 4;

        private static final int REQUEST_STATE_EMPTY = 0;
        private static final int REQUEST_STATE_READY = 1;
        private static final int REQUEST_STATE_PROCESSING = 2;
        private static final int REQUEST_STATE_CANCELLED = 3;

        private static final int RESPONSE_STATE_OFFSET = 4;
        private static final int RESPONSE_ID_OFFSET = 8;
        private static final int RESPONSE_PAYLOAD_LEN_OFFSET = 16;
        private static final int RESPONSE_PAYLOAD_OFFSET = 20;

        private static final int RESPONSE_STATE_EMPTY = 0;
        private static final int RESPONSE_STATE_SUCCESS = 1;
        private static final int RESPONSE_STATE_ERROR = 2;

        private static final class Slot {
            final MappedByteBuffer requestBuffer;
            final MappedByteBuffer responseBuffer;
            final FileChannel requestChannel;
            final FileChannel responseChannel;
            final Object lock = new Object();

            Slot(
                MappedByteBuffer requestBuffer,
                MappedByteBuffer responseBuffer,
                FileChannel requestChannel,
                FileChannel responseChannel
            ) {
                this.requestBuffer = requestBuffer;
                this.responseBuffer = responseBuffer;
                this.requestChannel = requestChannel;
                this.responseChannel = responseChannel;
            }
        }

        private final List<Slot> slots;

        private SharedMemoryBridge(List<Slot> slots) {
            this.slots = slots;
        }

        static SharedMemoryBridge open(Path runtimeDir, int slotCount) throws IOException {
            Path ipcDir = runtimeDir.resolve("ipc");
            Files.createDirectories(ipcDir);

            List<Slot> slots = new ArrayList<>(slotCount);
            for (int slotIndex = 0; slotIndex < slotCount; slotIndex++) {
                Path requestPath = ipcDir.resolve("chunk-request-" + slotIndex + ".mmap");
                Path responsePath = ipcDir.resolve("chunk-response-" + slotIndex + ".mmap");

                FileChannel requestChannel = FileChannel.open(
                    requestPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
                );
                FileChannel responseChannel = FileChannel.open(
                    responsePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
                );

                requestChannel.truncate(REQUEST_FILE_SIZE);
                responseChannel.truncate(RESPONSE_FILE_SIZE);

                MappedByteBuffer requestBuffer = requestChannel.map(FileChannel.MapMode.READ_WRITE, 0, REQUEST_FILE_SIZE);
                requestBuffer.order(ByteOrder.BIG_ENDIAN);
                MappedByteBuffer responseBuffer = responseChannel.map(FileChannel.MapMode.READ_WRITE, 0, RESPONSE_FILE_SIZE);
                responseBuffer.order(ByteOrder.BIG_ENDIAN);

                Slot slot = new Slot(requestBuffer, responseBuffer, requestChannel, responseChannel);
                initializeHeaders(slot);
                slots.add(slot);
            }
            return new SharedMemoryBridge(slots);
        }

        int slotCount() {
            return this.slots.size();
        }

        ChunkRequest tryClaimRequest(int slotIndex) {
            Slot slot = this.slots.get(slotIndex);
            synchronized (slot.lock) {
                if (slot.requestBuffer.getInt(0) != REQUEST_MAGIC) {
                    initializeHeaders(slot);
                    return null;
                }

                int state = slot.requestBuffer.getInt(REQUEST_STATE_OFFSET);
                if (state != REQUEST_STATE_READY) return null;

                long requestId = slot.requestBuffer.getLong(REQUEST_ID_OFFSET);
                int worldLen = slot.requestBuffer.getInt(REQUEST_WORLD_LEN_OFFSET);
                if (worldLen < 0) worldLen = 0;
                if (worldLen > REQUEST_WORLD_MAX_BYTES) worldLen = REQUEST_WORLD_MAX_BYTES;

                byte[] worldBytes = new byte[worldLen];
                readBytes(slot.requestBuffer, REQUEST_WORLD_OFFSET, worldBytes);
                String worldKey = new String(worldBytes, StandardCharsets.UTF_8);
                int chunkX = slot.requestBuffer.getInt(REQUEST_CHUNK_X_OFFSET);
                int chunkZ = slot.requestBuffer.getInt(REQUEST_CHUNK_Z_OFFSET);

                slot.requestBuffer.putInt(REQUEST_STATE_OFFSET, REQUEST_STATE_PROCESSING);
                return new ChunkRequest(slotIndex, requestId, worldKey, chunkX, chunkZ);
            }
        }

        void completeSuccess(ChunkRequest request, ChunkPayload payload) {
            Slot slot = this.slots.get(request.slotIndex());
            synchronized (slot.lock) {
                if (!isRequestActive(slot, request)) {
                    return;
                }
                byte[] responseBytes;
                try {
                    responseBytes = encodePayload(payload);
                } catch (IOException exception) {
                    completeError(request, "Failed to encode chunk payload");
                    return;
                }

                if (responseBytes.length > (RESPONSE_FILE_SIZE - RESPONSE_PAYLOAD_OFFSET)) {
                    completeError(request, "Chunk payload too large: " + responseBytes.length);
                    return;
                }
                // Drop right before writing response if request got cancelled while payload was being encoded.
                if (!isRequestActive(slot, request)) {
                    return;
                }

                writeResponse(slot, RESPONSE_STATE_SUCCESS, request.requestId(), responseBytes);
            }
        }

        void completeError(ChunkRequest request, String message) {
            Slot slot = this.slots.get(request.slotIndex());
            synchronized (slot.lock) {
                if (!isRequestActive(slot, request)) {
                    return;
                }
                byte[] payload = message.getBytes(StandardCharsets.UTF_8);
                if (payload.length > (RESPONSE_FILE_SIZE - RESPONSE_PAYLOAD_OFFSET)) {
                    payload = "bridge_error".getBytes(StandardCharsets.UTF_8);
                }
                // Drop right before writing response if request is no longer active.
                if (!isRequestActive(slot, request)) {
                    return;
                }
                writeResponse(slot, RESPONSE_STATE_ERROR, request.requestId(), payload);
            }
        }

        boolean isRequestActive(ChunkRequest request) {
            Slot slot = this.slots.get(request.slotIndex());
            synchronized (slot.lock) {
                return isRequestActive(slot, request);
            }
        }

        void close() {
            for (Slot slot : this.slots) {
                synchronized (slot.lock) {
                    try {
                        slot.requestChannel.close();
                    } catch (IOException ignored) {
                    }
                    try {
                        slot.responseChannel.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }

        private static void initializeHeaders(Slot slot) {
            slot.requestBuffer.putInt(0, REQUEST_MAGIC);
            slot.requestBuffer.putInt(REQUEST_STATE_OFFSET, REQUEST_STATE_EMPTY);
            slot.requestBuffer.putLong(REQUEST_ID_OFFSET, 0L);
            slot.requestBuffer.putInt(REQUEST_WORLD_LEN_OFFSET, 0);
            slot.requestBuffer.putInt(REQUEST_CHUNK_X_OFFSET, 0);
            slot.requestBuffer.putInt(REQUEST_CHUNK_Z_OFFSET, 0);

            slot.responseBuffer.putInt(0, RESPONSE_MAGIC);
            slot.responseBuffer.putInt(RESPONSE_STATE_OFFSET, RESPONSE_STATE_EMPTY);
            slot.responseBuffer.putLong(RESPONSE_ID_OFFSET, 0L);
            slot.responseBuffer.putInt(RESPONSE_PAYLOAD_LEN_OFFSET, 0);
        }

        private static byte[] encodePayload(ChunkPayload payload) throws IOException {
            ByteArrayOutputStream output = new ByteArrayOutputStream(65536);
            DataOutputStream data = new DataOutputStream(output);

            data.writeInt(payload.heightmaps().size());
            for (HeightmapPayload heightmap : payload.heightmaps()) {
                data.writeInt(heightmap.typeId());
                data.writeInt(heightmap.values().length);
                for (long value : heightmap.values()) {
                    data.writeLong(value);
                }
            }

            data.writeInt(payload.chunkData().length);
            data.write(payload.chunkData());
            data.writeInt(payload.snapshotSections().size());
            for (SnapshotSectionPayload section : payload.snapshotSections()) {
                data.writeInt(section.bitsPerEntry());
                data.writeInt(section.palette().length);
                for (int stateId : section.palette()) {
                    data.writeInt(stateId);
                }
                data.writeInt(section.storage().length);
                for (long value : section.storage()) {
                    data.writeLong(value);
                }
            }
            writeLongArray(data, payload.skyLightMask());
            writeLongArray(data, payload.blockLightMask());
            writeLongArray(data, payload.emptySkyLightMask());
            writeLongArray(data, payload.emptyBlockLightMask());
            writeByteArrayList(data, payload.skyLight());
            writeByteArrayList(data, payload.blockLight());

            data.flush();
            return output.toByteArray();
        }

        private static void writeLongArray(DataOutputStream data, long[] values) throws IOException {
            data.writeInt(values.length);
            for (long value : values) {
                data.writeLong(value);
            }
        }

        private static void writeByteArrayList(DataOutputStream data, List<byte[]> list) throws IOException {
            data.writeInt(list.size());
            for (byte[] entry : list) {
                data.writeInt(entry.length);
                data.write(entry);
            }
        }

        private static void writeResponse(Slot slot, int state, long requestId, byte[] payload) {
            slot.responseBuffer.putLong(RESPONSE_ID_OFFSET, requestId);
            slot.responseBuffer.putInt(RESPONSE_PAYLOAD_LEN_OFFSET, payload.length);
            writeBytes(slot.responseBuffer, RESPONSE_PAYLOAD_OFFSET, payload);
            slot.responseBuffer.putInt(RESPONSE_STATE_OFFSET, state);
        }

        private static boolean isRequestActive(Slot slot, ChunkRequest request) {
            return slot.requestBuffer.getInt(REQUEST_STATE_OFFSET) == REQUEST_STATE_PROCESSING
                && slot.requestBuffer.getLong(REQUEST_ID_OFFSET) == request.requestId();
        }

        private static void readBytes(MappedByteBuffer buffer, int offset, byte[] destination) {
            ByteBuffer duplicate = buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
            duplicate.position(offset);
            duplicate.get(destination);
        }

        private static void writeBytes(MappedByteBuffer buffer, int offset, byte[] source) {
            ByteBuffer duplicate = buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
            duplicate.position(offset);
            duplicate.put(source);
        }

        record ChunkRequest(int slotIndex, long requestId, String worldKey, int chunkX, int chunkZ) {
        }
    }
}
