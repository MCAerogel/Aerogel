package buildtools

import net.fabricmc.mappingio.MappingReader
import net.fabricmc.mappingio.adapter.MappingSourceNsSwitch
import net.fabricmc.mappingio.format.tiny.Tiny2FileWriter
import java.io.Writer
import java.nio.file.Files
import java.nio.file.Path

object ProguardToTinyConverter {
    fun convertToReversedTiny(inputProguard: Path, outputTiny: Path) {
        Files.createDirectories(outputTiny.parent)
        Files.newBufferedWriter(outputTiny).use { writer: Writer ->
            Tiny2FileWriter(writer, false).use { tinyWriter ->
                val switchToOfficialSource = MappingSourceNsSwitch(tinyWriter, "target")
                MappingReader.read(inputProguard, switchToOfficialSource)
            }
        }
    }
}
