#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import re
import urllib.request
from pathlib import Path


VERSION_MANIFEST_URL = "https://launchermeta.mojang.com/mc/game/version_manifest_v2.json"
DEFAULT_SOUND_KEY = "minecraft:ui.button.click"


def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url) as response:
        return json.load(response)


def discover_target_version(repo_root: Path) -> str | None:
    build_file = repo_root / "build.gradle.kts"
    if not build_file.exists():
        return None
    text = build_file.read_text(encoding="utf-8")
    match = re.search(r'paperDevBundle\("([0-9]+\.[0-9]+(?:\.[0-9]+)?)\-R', text)
    return match.group(1) if match else None


def resolve_version_manifest(target_version: str | None) -> tuple[str, str]:
    manifest = fetch_json(VERSION_MANIFEST_URL)
    versions = manifest.get("versions", [])
    latest_release = manifest.get("latest", {}).get("release")
    if target_version:
        for version in versions:
            if version.get("id") == target_version:
                return target_version, version["url"]
    if latest_release:
        for version in versions:
            if version.get("id") == latest_release:
                return latest_release, version["url"]
    raise RuntimeError("Unable to resolve a Minecraft version from Mojang version manifest.")


def resolve_sounds_json_url(version_meta_url: str) -> tuple[str, str]:
    version_meta = fetch_json(version_meta_url)
    asset_index_url = version_meta["assetIndex"]["url"]
    asset_index = fetch_json(asset_index_url)
    sounds_object = asset_index.get("objects", {}).get("minecraft/sounds.json")
    if not sounds_object:
        raise RuntimeError("minecraft/sounds.json is missing from asset index.")
    sound_hash = sounds_object["hash"]
    sounds_json_url = f"https://resources.download.minecraft.net/{sound_hash[:2]}/{sound_hash}"
    return sounds_json_url, sound_hash


def enum_name_for_key(sound_key: str) -> str:
    namespace, _, path = sound_key.partition(":")
    base = path if namespace == "minecraft" else f"{namespace}_{path}"
    token = re.sub(r"[^a-zA-Z0-9]+", "_", base).strip("_").upper()
    if not token:
        token = "UNKNOWN_SOUND"
    if token[0].isdigit():
        token = f"SOUND_{token}"
    return token


def dedupe_names(sound_keys: list[str]) -> list[tuple[str, str]]:
    used: dict[str, int] = {}
    out: list[tuple[str, str]] = []
    for key in sound_keys:
        base = enum_name_for_key(key)
        index = used.get(base, 0)
        name = base if index == 0 else f"{base}_{index}"
        used[base] = index + 1
        out.append((name, key))
    return out


def generate_sound_kt(version: str, sound_hash: str, constants: list[tuple[str, str]]) -> str:
    lines: list[str] = []
    lines.append("package org.macaroon3145.api.type")
    lines.append("")
    lines.append("// Generated from Mojang vanilla assets minecraft/sounds.json.")
    lines.append(f"// Minecraft version: {version}")
    lines.append(f"// sounds.json hash: {sound_hash}")
    lines.append(f"// Generated at (UTC): {dt.datetime.now(dt.timezone.utc).isoformat()}")
    lines.append("enum class Sound(val key: String) {")

    for i, (name, key) in enumerate(constants):
        suffix = ";" if i == len(constants) - 1 else ","
        lines.append(f'    {name}("{key}"){suffix}')

    lines.append("")
    lines.append("    companion object {")
    lines.append("        private val byKey: Map<String, Sound> = entries.associateBy { normalizeKey(it.key) }")
    lines.append("")
    lines.append("        val DEFAULT: Sound = fromKey(\"%s\") ?: entries.first()" % DEFAULT_SOUND_KEY)
    lines.append("")
    lines.append("        fun fromKey(key: String): Sound? = byKey[normalizeKey(key)]")
    lines.append("")
    lines.append("        fun normalizeKey(raw: String): String {")
    lines.append("            val trimmed = raw.trim()")
    lines.append("            if (trimmed.isEmpty()) return trimmed")
    lines.append("            val lower = trimmed.lowercase()")
    lines.append('            return if (\':\' in lower) lower else "minecraft:$lower"')
    lines.append("        }")
    lines.append("    }")
    lines.append("}")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Sound enum from Mojang vanilla sounds.json")
    parser.add_argument(
        "--version",
        type=str,
        default=None,
        help="Target Minecraft version (e.g. 1.21.11). If omitted, uses build.gradle paperDevBundle version or latest release.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("modules/api/src/main/kotlin/org/macaroon3145/api/type/Sound.kt"),
        help="Output Kotlin file path.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[3]
    requested_version = args.version or discover_target_version(repo_root)
    version_id, version_meta_url = resolve_version_manifest(requested_version)
    sounds_url, sound_hash = resolve_sounds_json_url(version_meta_url)
    sounds_json = fetch_json(sounds_url)

    sound_keys = sorted(f"minecraft:{k}" for k in sounds_json.keys())
    constants = dedupe_names(sound_keys)
    output_text = generate_sound_kt(version_id, sound_hash, constants)

    output_path = (repo_root / args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output_text, encoding="utf-8")

    print(f"Generated {len(constants)} sounds into {output_path}")
    print(f"Version: {version_id}")
    print(f"Source: {sounds_url}")


if __name__ == "__main__":
    main()
