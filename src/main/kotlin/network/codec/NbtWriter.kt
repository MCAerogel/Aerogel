package org.macaroon3145.network.codec

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.DataOutputStream
import java.io.OutputStream
import kotlin.text.Charsets.UTF_8

object NbtWriter {
    fun writeAnonymousRoot(stream: OutputStream, tagNode: JsonObject) {
        val out = DataOutputStream(stream)
        val typeName = tagNode["type"]!!.jsonPrimitive.content
        out.writeByte(tagTypeId(typeName))
        writeTagPayload(out, typeName, tagNode["value"]!!)
    }

    private fun writeTagPayload(out: DataOutputStream, typeName: String, value: JsonElement) {
        when (typeName) {
            "end" -> {
            }
            "byte" -> out.writeByte(value.jsonPrimitive.content.toInt())
            "int" -> out.writeInt(value.jsonPrimitive.content.toInt())
            "float" -> out.writeFloat(value.jsonPrimitive.content.toFloat())
            "double" -> out.writeDouble(value.jsonPrimitive.content.toDouble())
            "string" -> writeNbtString(out, value.jsonPrimitive.content)
            "intArray" -> {
                val ints = value.jsonArray
                out.writeInt(ints.size)
                for (v in ints) out.writeInt(v.jsonPrimitive.content.toInt())
            }
            "list" -> writeListPayload(out, value.jsonObject)
            "compound" -> writeCompoundPayload(out, value.jsonObject)
            else -> error("Unsupported NBT type: $typeName")
        }
    }

    private fun writeListPayload(out: DataOutputStream, value: JsonObject) {
        val elementType = value["type"]!!.jsonPrimitive.content
        val list = value["value"]!!.jsonArray
        out.writeByte(tagTypeId(elementType))
        out.writeInt(list.size)
        for (element in list) {
            writeListElementPayload(out, elementType, element)
        }
    }

    private fun writeListElementPayload(out: DataOutputStream, elementType: String, element: JsonElement) {
        when (elementType) {
            "byte" -> out.writeByte(element.jsonPrimitive.content.toInt())
            "int" -> out.writeInt(element.jsonPrimitive.content.toInt())
            "float" -> out.writeFloat(element.jsonPrimitive.content.toFloat())
            "double" -> out.writeDouble(element.jsonPrimitive.content.toDouble())
            "string" -> writeNbtString(out, element.jsonPrimitive.content)
            "compound" -> writeCompoundPayload(out, element.jsonObject)
            "intArray" -> {
                val ints = element.jsonArray
                out.writeInt(ints.size)
                for (v in ints) out.writeInt(v.jsonPrimitive.content.toInt())
            }
            "list" -> writeListPayload(out, element.jsonObject)
            "end" -> {
            }
            else -> error("Unsupported list element NBT type: $elementType")
        }
    }

    private fun writeCompoundPayload(out: DataOutputStream, value: JsonObject) {
        for ((name, tagNodeElement) in value) {
            val tagNode = tagNodeElement.jsonObject
            val childType = tagNode["type"]!!.jsonPrimitive.content
            out.writeByte(tagTypeId(childType))
            writeNbtString(out, name)
            writeTagPayload(out, childType, tagNode["value"]!!)
        }
        out.writeByte(0)
    }

    private fun tagTypeId(typeName: String): Int {
        return when (typeName) {
            "end" -> 0
            "byte" -> 1
            "short" -> 2
            "int" -> 3
            "long" -> 4
            "float" -> 5
            "double" -> 6
            "byteArray" -> 7
            "string" -> 8
            "list" -> 9
            "compound" -> 10
            "intArray" -> 11
            "longArray" -> 12
            else -> error("Unknown NBT tag type: $typeName")
        }
    }

    private fun writeNbtString(out: DataOutputStream, value: String) {
        val bytes = value.toByteArray(UTF_8)
        out.writeShort(bytes.size)
        out.write(bytes)
    }
}
