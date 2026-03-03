package org.macaroon3145.api.type

interface TypeRegistry {
    fun itemById(id: Int): ItemType?
    fun itemByKey(key: String): ItemType?
    fun blockById(id: Int): BlockType?
    fun blockByKey(key: String): BlockType?
    fun allItems(): List<ItemType>
    fun allBlocks(): List<BlockType>
}
