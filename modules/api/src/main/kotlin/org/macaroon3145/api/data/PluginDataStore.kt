package org.macaroon3145.api.data

import java.nio.file.Path
import java.util.concurrent.CompletableFuture

interface PluginDataStore {
    val dataDirectory: Path

    fun resolve(key: String): Path

    fun saveBytes(key: String, bytes: ByteArray): CompletableFuture<Unit>

    fun loadBytes(key: String): CompletableFuture<ByteArray?>

    fun delete(key: String): CompletableFuture<Boolean>
}
