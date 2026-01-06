package dev.sockmower.misguidedmod;

import static java.nio.file.StandardOpenOption.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.Logger;
import org.lwjgl.BufferUtils;

import dev.sockmower.misguidedmod.repack.io.airlift.compress.zstd.ZstdCompressor;
import dev.sockmower.misguidedmod.repack.io.airlift.compress.zstd.ZstdDecompressor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import net.minecraft.network.PacketBuffer;
import net.minecraft.network.play.server.SPacketChunkData;

public class CachedRegion implements Closeable {
    public static final int CHUNKS_PER_REGION = 32 * 32;

    private Path regionFile;
    private SeekableByteChannel seekableStream;
    private Int2ObjectMap<ChunkHeader> lookupTable = new Int2ObjectOpenHashMap<>();
    private Logger logger;
    public long lastAccessed;
    public boolean poison;

    private final Object ioLock = new Object();

    CachedRegion(boolean poison) {
        this.poison = poison;
    }

    CachedRegion(String directory, int x, int z, Logger logger) throws IOException {
        this.logger = logger;

        regionFile = Paths.get(String.format("%s\\r.%d.%d.mmr", directory, x, z));
        boolean fileExisted = Files.exists(regionFile);

        if (!fileExisted) {
            Files.createFile(regionFile);
        }

        seekableStream = Files.newByteChannel(regionFile, WRITE, READ);

        if (!fileExisted) {
            byte[] write = new byte[CHUNKS_PER_REGION * 8];

            seekableStream.write(ByteBuffer.wrap(write));
        }

        for (int i = 0; i < CHUNKS_PER_REGION; i++) {
            ByteBuffer header = BufferUtils.createByteBuffer(8);
            seekableStream.position(i * 8);
            readFully(seekableStream, header);

            lookupTable.put(i, new ChunkHeader(header.getInt(0), header.getInt(4), i * 8));
        }
    }

    ChunkHeader getChunkHeader(Pos2 pos) throws IOException {
        return lookupTable.get((pos.x & 31) | ((pos.z & 31) << 5));
    }

    private static void readFully(SeekableByteChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int r = ch.read(buf);
            if (r < 0) throw new IOException("Unexpected EOF");
            if (r == 0) Thread.yield();
        }
    }

    CachedChunk getChunk(Pos2 pos) throws IOException {
        ChunkHeader header = getChunkHeader(pos);
        if (header == null || !header.chunkExists() || header.size <= 0) {
            return null;
        }

        ByteBuffer compressed;

        synchronized (ioLock) {
            seekableStream.position(header.offset);
            compressed = BufferUtils.createByteBuffer(header.size);
            readFully(seekableStream, compressed);
            compressed.flip();
        }

        try {
            ZstdDecompressor decompressor = new ZstdDecompressor();

            long decompressedSize = ZstdDecompressor.getDecompressedSize(compressed);
            if (decompressedSize <= 0 || decompressedSize > (128L * 1024 * 1024)) { // защита от мусора
                logger.warn("Bad decompressed size {} at {} (off={}, sz={})",
                        decompressedSize, pos, header.offset, header.size);
                return null;
            }

            ByteBuffer decompressed = BufferUtils.createByteBuffer((int) decompressedSize);
            decompressor.decompress(compressed, decompressed);

            ByteBuf buffer = Unpooled.buffer(decompressed.position());
            decompressed.flip();
            buffer.writeBytes(decompressed);
            buffer.readerIndex(0);

            SPacketChunkData chunkPacket = new SPacketChunkData();
            chunkPacket.readPacketData(new PacketBuffer(buffer));

            return new CachedChunk(pos, chunkPacket);
        } catch (dev.sockmower.misguidedmod.repack.io.airlift.compress.MalformedInputException ex) {
            // Это и есть твои "Invalid magic prefix" / "Not enough input bytes"
            logger.warn("Corrupt cached chunk at {} (off={}, sz={}): {}",
                    pos, header.offset, header.size, ex.toString());
            return null;
        } catch (Exception ex) {
            logger.warn("Malformed chunk at {} (off={}, sz={})", pos, header.offset, header.size, ex);
            return null;
        }
    }
    void writeHeader(ChunkHeader head) throws IOException {
        lookupTable.put((int) (head.headerOffset / 8), head);

        seekableStream.position(head.headerOffset);

        ByteBuffer header = BufferUtils.createByteBuffer(8);
        header.putInt(head.offset);
        header.putInt(head.size);

        seekableStream.write((ByteBuffer) header.flip());
    }

    private ChunkHeader findMaxChunk() {
        ChunkHeader max = new ChunkHeader(0, CHUNKS_PER_REGION * 8, -1);
        for (int i = 0; i < CHUNKS_PER_REGION; i++) {
            ChunkHeader head = lookupTable.get(i);
            if (head != null && head.chunkExists() && head.offset > max.offset) {
                max = head;
            }
        }
        return max;
    }

    void writeChunk(CachedChunk chunk) throws IOException {
        synchronized (ioLock) {
            ChunkHeader header = getChunkHeader(chunk.pos);

            PacketBuffer buffer = new PacketBuffer(Unpooled.buffer());
            chunk.packet.writePacketData(buffer);
            int rawSize = buffer.readableBytes();

            ByteBuffer chunkData = BufferUtils.createByteBuffer(rawSize);
            buffer.readBytes(chunkData);
            chunkData.flip();

            ZstdCompressor compressor = new ZstdCompressor();
            ByteBuffer compressed = BufferUtils.createByteBuffer(compressor.maxCompressedLength(rawSize));
            compressor.compress(chunkData, compressed);

            int newSize = compressed.position();
            compressed.flip();

            if (header.chunkExists()) {
                if (newSize <= header.size) {
                    seekableStream.position(header.offset);
                    seekableStream.write(compressed);

                    header.size = newSize;
                    writeHeader(header);
                } else {
                    ChunkHeader max = findMaxChunk();
                    int newOffset = max.offset + max.size;

                    seekableStream.position(newOffset);
                    seekableStream.write(compressed);

                    header.offset = newOffset;
                    header.size = newSize;
                    writeHeader(header);
                }
            } else {
                ChunkHeader max = findMaxChunk();
                int newOffset = max.offset + max.size;

                seekableStream.position(newOffset);
                seekableStream.write(compressed);

                header.offset = newOffset;
                header.size = newSize;
                writeHeader(header);
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (ioLock) {
            seekableStream.close();
        }
    }
}
