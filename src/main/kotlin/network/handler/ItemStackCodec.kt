package org.macaroon3145.network.handler

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.macaroon3145.network.NetworkUtils
import java.io.ByteArrayOutputStream

object ItemStackCodec {
    const val ITEM_COMPONENT_CUSTOM_NAME_ID = 6
    const val ITEM_COMPONENT_LORE_ID = 11
    const val ITEM_COMPONENT_BLOCK_ENTITY_DATA_ID = 58
    const val ITEM_COMPONENT_CONTAINER_ID = 73

    data class DecodedItemStack(
        val itemId: Int,
        val count: Int,
        val shulkerContents: ChestState?
    )

    data class DecodedBlockEntityData(
        val blockEntityTypeId: Int,
        val nbtPayload: ByteArray
    )

    fun encodeSimple(itemId: Int, count: Int = 1): ByteArray {
        val out = ByteArrayOutputStream(8)
        NetworkUtils.writeVarInt(out, count.coerceAtLeast(1))
        NetworkUtils.writeVarInt(out, itemId)
        NetworkUtils.writeVarInt(out, 0)
        NetworkUtils.writeVarInt(out, 0)
        return out.toByteArray()
    }

    fun encode(
        itemId: Int,
        count: Int = 1,
        customNamePayload: ByteArray? = null,
        lorePayload: ByteArray? = null,
        shulkerContents: ChestState? = null,
        blockEntityTypeId: Int = -1,
        blockEntityDataPayload: ByteArray? = null,
        maxStackSize: Int = 64,
        includeShulkerComponent: Boolean = false
    ): ByteArray {
        val out = ByteArrayOutputStream(8)
        NetworkUtils.writeVarInt(out, count.coerceAtLeast(1))
        NetworkUtils.writeVarInt(out, itemId)

        val hasName = customNamePayload != null
        val hasLore = lorePayload != null
        val normalizedShulker = if (includeShulkerComponent) {
            shulkerContents?.takeIf(::hasAnyItems)
        } else {
            null
        }
        val hasShulker = normalizedShulker != null
        val hasBlockEntityData = blockEntityTypeId >= 0 && blockEntityDataPayload != null && blockEntityDataPayload.isNotEmpty()

        val added = (if (hasName) 1 else 0) +
            (if (hasLore) 1 else 0) +
            (if (hasShulker) 1 else 0) +
            (if (hasBlockEntityData) 1 else 0)
        NetworkUtils.writeVarInt(out, added)
        NetworkUtils.writeVarInt(out, 0) // removed component count

        if (customNamePayload != null) {
            NetworkUtils.writeVarInt(out, ITEM_COMPONENT_CUSTOM_NAME_ID)
            out.write(customNamePayload)
        }
        if (lorePayload != null) {
            NetworkUtils.writeVarInt(out, ITEM_COMPONENT_LORE_ID)
            out.write(lorePayload)
        }
        if (normalizedShulker != null) {
            NetworkUtils.writeVarInt(out, ITEM_COMPONENT_CONTAINER_ID)
            out.write(encodeShulkerContainerComponentPayload(normalizedShulker, maxStackSize))
        }
        if (hasBlockEntityData) {
            NetworkUtils.writeVarInt(out, ITEM_COMPONENT_BLOCK_ENTITY_DATA_ID)
            NetworkUtils.writeVarInt(out, blockEntityTypeId)
            out.write(blockEntityDataPayload)
        }
        return out.toByteArray()
    }

    fun decodeUntrustedBlockEntityData(
        encodedItemStack: ByteArray
    ): DecodedBlockEntityData? {
        if (encodedItemStack.isEmpty()) return null
        val buf = Unpooled.wrappedBuffer(encodedItemStack)
        return try {
            val count = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
            if (count <= 0) return null
            val rawItemId = NetworkUtils.readVarInt(buf)
            if (rawItemId < 0) return null
            val addedComponents = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
            val removedComponents = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
            var decoded: DecodedBlockEntityData? = null
            repeat(addedComponents) {
                val componentType = NetworkUtils.readVarInt(buf)
                val payloadLength = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
                if (buf.readableBytes() < payloadLength) {
                    throw IllegalStateException("Malformed UntrustedSlot component payload length")
                }
                val payload = ByteArray(payloadLength)
                buf.readBytes(payload)
                if (decoded == null && componentType == ITEM_COMPONENT_BLOCK_ENTITY_DATA_ID) {
                    decoded = decodeBlockEntityDataComponentPayload(payload)
                }
            }
            repeat(removedComponents) {
                NetworkUtils.readVarInt(buf)
            }
            decoded
        } catch (_: Throwable) {
            null
        } finally {
            buf.release()
        }
    }

    fun decode(
        encodedItemStack: ByteArray,
        maxStackSize: Int = 64,
        shouldExtractShulker: (Int) -> Boolean = { true }
    ): DecodedItemStack {
        if (encodedItemStack.isEmpty()) {
            return DecodedItemStack(itemId = -1, count = 0, shulkerContents = null)
        }
        val buf = Unpooled.wrappedBuffer(encodedItemStack)
        return try {
            val count = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
            if (count <= 0) {
                return DecodedItemStack(itemId = -1, count = 0, shulkerContents = null)
            }
            val rawItemId = NetworkUtils.readVarInt(buf)
            val itemId = if (rawItemId < 0) -1 else rawItemId
            if (itemId < 0) {
                return DecodedItemStack(itemId = -1, count = 0, shulkerContents = null)
            }
            val shulker = runCatching {
                val addedComponents = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
                val modernStart = buf.readerIndex()
                val modern = runCatching {
                    readShulkerContentsFromComponentSection(
                        buf = buf,
                        addedComponents = addedComponents,
                        maxStackSize = maxStackSize,
                        removedComponentsBeforePayload = true
                    )
                }.getOrNull()
                if (modern != null || !shouldExtractShulker(itemId)) {
                    modern
                } else {
                    buf.readerIndex(modernStart)
                    runCatching {
                        readShulkerContentsFromComponentSection(
                            buf = buf,
                            addedComponents = addedComponents,
                            maxStackSize = maxStackSize,
                            removedComponentsBeforePayload = false
                        )
                    }.getOrNull()
                }
            }.getOrNull()
            DecodedItemStack(
                itemId = itemId,
                count = count.coerceAtMost(maxStackSize),
                shulkerContents = shulker?.takeIf(::hasAnyItems)
            )
        } catch (_: Throwable) {
            DecodedItemStack(itemId = -1, count = 0, shulkerContents = null)
        } finally {
            buf.release()
        }
    }

    fun decodeUntrusted(
        encodedItemStack: ByteArray,
        maxStackSize: Int = 64,
        shouldExtractShulker: (Int) -> Boolean = { true }
    ): DecodedItemStack {
        if (encodedItemStack.isEmpty()) {
            return DecodedItemStack(itemId = -1, count = 0, shulkerContents = null)
        }
        val buf = Unpooled.wrappedBuffer(encodedItemStack)
        return try {
            val count = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
            if (count <= 0) {
                return DecodedItemStack(itemId = -1, count = 0, shulkerContents = null)
            }
            val rawItemId = NetworkUtils.readVarInt(buf)
            val itemId = if (rawItemId < 0) -1 else rawItemId
            if (itemId < 0) {
                return DecodedItemStack(itemId = -1, count = 0, shulkerContents = null)
            }
            val addedComponents = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
            val removedComponents = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
            var extracted: ChestState? = null
            repeat(addedComponents) {
                val componentType = NetworkUtils.readVarInt(buf)
                val payloadLength = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
                if (buf.readableBytes() < payloadLength) {
                    throw IllegalStateException("Malformed UntrustedSlot component payload length")
                }
                val payload = ByteArray(payloadLength)
                buf.readBytes(payload)
                if (extracted == null && shouldExtractShulker(itemId)) {
                    extracted = readShulkerContentsFromComponentPayload(
                        componentType = componentType,
                        payload = payload,
                        maxStackSize = maxStackSize
                    ) ?: extracted
                }
            }
            repeat(removedComponents) {
                NetworkUtils.readVarInt(buf)
            }
            DecodedItemStack(
                itemId = itemId,
                count = count.coerceAtMost(maxStackSize),
                shulkerContents = extracted?.takeIf(::hasAnyItems)
            )
        } catch (_: Throwable) {
            DecodedItemStack(itemId = -1, count = 0, shulkerContents = null)
        } finally {
            buf.release()
        }
    }

    fun empty(): ByteArray {
        val out = ByteArrayOutputStream(1)
        NetworkUtils.writeVarInt(out, 0)
        return out.toByteArray()
    }

    private fun encodeShulkerContainerComponentPayload(chest: ChestState, maxStackSize: Int): ByteArray {
        val out = ByteArrayOutputStream(512)
        NetworkUtils.writeVarInt(out, 27)
        for (i in 0 until 27) {
            val id = chest.itemIds[i]
            val count = chest.itemCounts[i]
            if (id >= 0 && count > 0) {
                out.write(encodeSimple(id, count.coerceAtMost(maxStackSize)))
            } else {
                out.write(empty())
            }
        }
        return out.toByteArray()
    }

    private fun hasAnyItems(chest: ChestState): Boolean {
        for (i in chest.itemIds.indices) {
            if (chest.itemIds[i] >= 0 && chest.itemCounts[i] > 0) return true
        }
        return false
    }

    private fun readShulkerContentsFromComponentSection(
        buf: ByteBuf,
        addedComponents: Int,
        maxStackSize: Int,
        removedComponentsBeforePayload: Boolean
    ): ChestState? {
        var extracted: ChestState? = null
        if (removedComponentsBeforePayload) {
            val removedComponents = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
            repeat(addedComponents) {
                val componentType = NetworkUtils.readVarInt(buf)
                if (componentType == ITEM_COMPONENT_CONTAINER_ID) {
                    extracted = readShulkerContainerComponent(buf, maxStackSize)
                } else if (componentType == ITEM_COMPONENT_BLOCK_ENTITY_DATA_ID) {
                    if (extracted == null) {
                        extracted = readShulkerFromBlockEntityDataComponent(buf, maxStackSize)
                    } else if (!skipBlockEntityDataComponentPayload(buf)) {
                        throw IllegalStateException("Unsupported slot component: $componentType")
                    }
                } else if (!skipSlotComponentData(buf, componentType, maxStackSize)) {
                    throw IllegalStateException("Unsupported slot component: $componentType")
                }
            }
            repeat(removedComponents) {
                NetworkUtils.readVarInt(buf)
            }
        } else {
            repeat(addedComponents) {
                val componentType = NetworkUtils.readVarInt(buf)
                if (componentType == ITEM_COMPONENT_CONTAINER_ID) {
                    extracted = readShulkerContainerComponent(buf, maxStackSize)
                } else if (componentType == ITEM_COMPONENT_BLOCK_ENTITY_DATA_ID) {
                    if (extracted == null) {
                        extracted = readShulkerFromBlockEntityDataComponent(buf, maxStackSize)
                    } else if (!skipBlockEntityDataComponentPayload(buf)) {
                        throw IllegalStateException("Unsupported slot component: $componentType")
                    }
                } else if (!skipSlotComponentData(buf, componentType, maxStackSize)) {
                    throw IllegalStateException("Unsupported slot component: $componentType")
                }
            }
            val removedComponents = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
            repeat(removedComponents) {
                NetworkUtils.readVarInt(buf)
            }
        }
        return extracted
    }

    private fun readShulkerContainerComponent(buf: ByteBuf, maxStackSize: Int): ChestState? {
        val ids = IntArray(27) { -1 }
        val counts = IntArray(27)
        val entries = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
        for (i in 0 until entries) {
            val slot = readSlotItemStackShallow(buf, maxStackSize) ?: return null
            if (i !in 0..26) continue
            if (slot.first >= 0 && slot.second > 0) {
                ids[i] = slot.first
                counts[i] = slot.second.coerceAtMost(maxStackSize)
            }
        }
        return ChestState(itemIds = ids, itemCounts = counts)
    }

    private fun readSlotItemStackShallow(buf: ByteBuf, maxStackSize: Int): Pair<Int, Int>? {
        val count = NetworkUtils.readVarInt(buf)
        if (count <= 0) return -1 to 0
        val itemId = NetworkUtils.readVarInt(buf)
        val addedComponents = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
        val modernStart = buf.readerIndex()
        val removedBeforeParsed = runCatching {
            val removedComponents = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
            repeat(addedComponents) {
                val componentType = NetworkUtils.readVarInt(buf)
                if (!skipSlotComponentData(buf, componentType, maxStackSize)) {
                    throw IllegalStateException("Unsupported slot component: $componentType")
                }
            }
            repeat(removedComponents) {
                NetworkUtils.readVarInt(buf)
            }
            true
        }.getOrDefault(false)
        if (!removedBeforeParsed) {
            buf.readerIndex(modernStart)
            runCatching {
                repeat(addedComponents) {
                    val componentType = NetworkUtils.readVarInt(buf)
                    if (!skipSlotComponentData(buf, componentType, maxStackSize)) {
                        throw IllegalStateException("Unsupported slot component: $componentType")
                    }
                }
                val removedComponents = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
                repeat(removedComponents) {
                    NetworkUtils.readVarInt(buf)
                }
            }.getOrElse {
                return null
            }
        }
        return itemId to count.coerceAtMost(maxStackSize)
    }

    private fun skipSlotComponentData(buf: ByteBuf, componentType: Int, maxStackSize: Int): Boolean {
        return when (componentType) {
            ITEM_COMPONENT_CUSTOM_NAME_ID -> skipAnonymousNbt(buf)
            ITEM_COMPONENT_LORE_ID -> {
                val loreLines = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
                repeat(loreLines) {
                    if (!skipAnonymousNbt(buf)) return false
                }
                true
            }
            ITEM_COMPONENT_CONTAINER_ID -> {
                val entries = NetworkUtils.readVarInt(buf).coerceAtLeast(0)
                repeat(entries) {
                    if (readSlotItemStackShallow(buf, maxStackSize) == null) return false
                }
                true
            }
            ITEM_COMPONENT_BLOCK_ENTITY_DATA_ID -> {
                skipBlockEntityDataComponentPayload(buf)
            }
            else -> false
        }
    }

    private fun readShulkerContentsFromComponentPayload(
        componentType: Int,
        payload: ByteArray,
        maxStackSize: Int
    ): ChestState? {
        val payloadBuf = Unpooled.wrappedBuffer(payload)
        return try {
            when (componentType) {
                ITEM_COMPONENT_CONTAINER_ID -> readShulkerContainerComponent(payloadBuf, maxStackSize)
                ITEM_COMPONENT_BLOCK_ENTITY_DATA_ID -> readShulkerFromBlockEntityDataComponent(payloadBuf, maxStackSize)
                else -> null
            }
        } catch (_: Throwable) {
            null
        } finally {
            payloadBuf.release()
        }
    }

    private fun decodeBlockEntityDataComponentPayload(payload: ByteArray): DecodedBlockEntityData? {
        val buf = Unpooled.wrappedBuffer(payload)
        return try {
            val typeId = NetworkUtils.readVarInt(buf)
            if (typeId < 0) return null
            val remaining = buf.readableBytes()
            if (remaining <= 0) return null
            val nbt = ByteArray(remaining)
            buf.readBytes(nbt)
            DecodedBlockEntityData(
                blockEntityTypeId = typeId,
                nbtPayload = nbt
            )
        } catch (_: Throwable) {
            null
        } finally {
            buf.release()
        }
    }

    private fun readShulkerFromBlockEntityDataComponent(buf: ByteBuf, maxStackSize: Int): ChestState? {
        // Structured component payload: { type: varint, data: anonymousNbt }
        NetworkUtils.readVarInt(buf)
        return readShulkerFromBlockEntityDataNbt(buf, maxStackSize)
    }

    private fun skipBlockEntityDataComponentPayload(buf: ByteBuf): Boolean {
        NetworkUtils.readVarInt(buf) // block entity type id
        return skipAnonymousNbt(buf) // data
    }

    private fun readShulkerFromBlockEntityDataNbt(buf: ByteBuf, maxStackSize: Int): ChestState? {
        if (!buf.isReadable) return null
        val start = buf.readerIndex()
        if (buf.readUnsignedByte().toInt() != 10) {
            buf.readerIndex(start)
            return null
        }
        val ids = IntArray(27) { -1 }
        val counts = IntArray(27)
        val parsed = readCompoundForItemsList(buf, ids, counts, maxStackSize, depth = 0)
        if (!parsed) {
            buf.readerIndex(start)
            return null
        }
        val chest = ChestState(itemIds = ids, itemCounts = counts)
        return chest.takeIf(::hasAnyItems)
    }

    private fun readCompoundForItemsList(
        buf: ByteBuf,
        ids: IntArray,
        counts: IntArray,
        maxStackSize: Int,
        depth: Int
    ): Boolean {
        if (depth > 64) return false
        while (true) {
            if (!buf.isReadable) return false
            val type = buf.readUnsignedByte().toInt()
            if (type == 0) return true
            val name = readNbtString(buf) ?: return false
            if (type == 9 && name == "Items") {
                if (!readItemsListPayload(buf, ids, counts, maxStackSize, depth + 1)) return false
            } else if (!skipNbtPayloadByType(buf, type, depth + 1)) {
                return false
            }
        }
    }

    private fun readItemsListPayload(
        buf: ByteBuf,
        ids: IntArray,
        counts: IntArray,
        maxStackSize: Int,
        depth: Int
    ): Boolean {
        if (depth > 64) return false
        if (buf.readableBytes() < 5) return false
        val elementType = buf.readUnsignedByte().toInt()
        val length = buf.readInt().coerceAtLeast(0)
        if (elementType != 10) {
            repeat(length) {
                if (!skipNbtPayloadByType(buf, elementType, depth + 1)) return false
            }
            return true
        }
        for (listIndex in 0 until length) {
            val item = readItemCompoundFromNbt(buf, depth + 1) ?: return false
            val slot = when {
                item.slot in 0..26 -> item.slot
                listIndex in 0..26 -> listIndex
                else -> -1
            }
            if (slot !in 0..26) continue
            if (item.itemId >= 0 && item.count > 0) {
                ids[slot] = item.itemId
                counts[slot] = item.count.coerceAtMost(maxStackSize)
            }
        }
        return true
    }

    private data class ParsedNbtItem(val slot: Int, val itemId: Int, val count: Int)

    private fun readItemCompoundFromNbt(buf: ByteBuf, depth: Int): ParsedNbtItem? {
        if (depth > 64) return null
        var slot = -1
        var itemId = -1
        var count = 0
        while (true) {
            if (!buf.isReadable) return null
            val type = buf.readUnsignedByte().toInt()
            if (type == 0) break
            val name = readNbtString(buf) ?: return null
            when {
                name == "Slot" && type == 1 -> {
                    if (buf.readableBytes() < 1) return null
                    slot = buf.readByte().toInt() and 0xFF
                }
                name == "id" && type == 8 -> {
                    val key = readNbtString(buf) ?: return null
                    itemId = PlayerSessionManager.itemIdForPersistence(key)
                }
                name == "Count" && type == 1 -> {
                    if (buf.readableBytes() < 1) return null
                    count = buf.readByte().toInt()
                }
                name == "Count" && type == 2 -> {
                    if (buf.readableBytes() < 2) return null
                    count = buf.readShort().toInt()
                }
                name == "Count" && type == 3 -> {
                    if (buf.readableBytes() < 4) return null
                    count = buf.readInt()
                }
                else -> {
                    if (!skipNbtPayloadByType(buf, type, depth + 1)) return null
                }
            }
        }
        return ParsedNbtItem(slot = slot, itemId = itemId, count = count)
    }

    private fun readNbtString(buf: ByteBuf): String? {
        if (buf.readableBytes() < 2) return null
        val length = buf.readUnsignedShort()
        if (buf.readableBytes() < length) return null
        val bytes = ByteArray(length)
        buf.readBytes(bytes)
        return bytes.toString(Charsets.UTF_8)
    }

    private fun skipAnonymousNbt(buf: ByteBuf): Boolean {
        if (!buf.isReadable) return false
        val rootType = buf.readUnsignedByte().toInt()
        if (rootType == 0) return true
        if (rootType != 10) return false
        return skipCompoundPayload(buf, depth = 0)
    }

    private fun skipCompoundPayload(buf: ByteBuf, depth: Int): Boolean {
        if (depth > 64) return false
        while (true) {
            if (!buf.isReadable) return false
            val type = buf.readUnsignedByte().toInt()
            if (type == 0) return true
            if (buf.readableBytes() < 2) return false
            val nameLength = buf.readUnsignedShort()
            if (buf.readableBytes() < nameLength) return false
            buf.skipBytes(nameLength)
            if (!skipNbtPayloadByType(buf, type, depth + 1)) return false
        }
    }

    private fun skipListPayload(buf: ByteBuf, depth: Int): Boolean {
        if (depth > 64) return false
        if (buf.readableBytes() < 5) return false
        val elementType = buf.readUnsignedByte().toInt()
        val length = buf.readInt().coerceAtLeast(0)
        repeat(length) {
            if (!skipNbtPayloadByType(buf, elementType, depth + 1)) return false
        }
        return true
    }

    private fun skipNbtPayloadByType(buf: ByteBuf, type: Int, depth: Int): Boolean {
        if (depth > 64) return false
        return when (type) {
            0 -> true
            1 -> if (buf.readableBytes() < 1) false else {
                buf.skipBytes(1)
                true
            }
            2 -> if (buf.readableBytes() < 2) false else {
                buf.skipBytes(2)
                true
            }
            3 -> if (buf.readableBytes() < 4) false else {
                buf.skipBytes(4)
                true
            }
            4 -> if (buf.readableBytes() < 8) false else {
                buf.skipBytes(8)
                true
            }
            5 -> if (buf.readableBytes() < 4) false else {
                buf.skipBytes(4)
                true
            }
            6 -> if (buf.readableBytes() < 8) false else {
                buf.skipBytes(8)
                true
            }
            7 -> {
                if (buf.readableBytes() < 4) return false
                val length = buf.readInt().coerceAtLeast(0)
                if (buf.readableBytes() < length) return false
                buf.skipBytes(length)
                true
            }
            8 -> {
                if (buf.readableBytes() < 2) return false
                val length = buf.readUnsignedShort().toInt()
                if (buf.readableBytes() < length) return false
                buf.skipBytes(length)
                true
            }
            9 -> skipListPayload(buf, depth + 1)
            10 -> skipCompoundPayload(buf, depth + 1)
            11 -> {
                if (buf.readableBytes() < 4) return false
                val length = buf.readInt().coerceAtLeast(0)
                val bytes = length * 4L
                if (bytes > Int.MAX_VALUE || buf.readableBytes() < bytes.toInt()) return false
                buf.skipBytes(bytes.toInt())
                true
            }
            12 -> {
                if (buf.readableBytes() < 4) return false
                val length = buf.readInt().coerceAtLeast(0)
                val bytes = length * 8L
                if (bytes > Int.MAX_VALUE || buf.readableBytes() < bytes.toInt()) return false
                buf.skipBytes(bytes.toInt())
                true
            }
            else -> false
        }
    }
}
