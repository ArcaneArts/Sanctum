"""Compare closed SQLite snapshots and measure an ordered page delta."""

import argparse
import filecmp
import json
from pathlib import Path
import shutil
import struct
import tempfile
import time


HEADER = struct.Struct(">4sIQ")
PAGE_NUMBER = struct.Struct(">Q")
MAGIC = b"SPD1"


def sqlite_page_size(file):
    header = file.read(100)
    if len(header) != 100 or header[:16] != b"SQLite format 3\x00":
        raise ValueError("Expected a closed SQLite backup")
    if header[18:20] != b"\x01\x01":
        raise ValueError("Use SQLite backups in DELETE journal mode")
    file.seek(0)
    size = int.from_bytes(header[16:18], "big")
    if size == 1:
        return 65536
    if size < 512 or size > 32768 or size & (size - 1):
        raise ValueError("Invalid SQLite page size")
    return size


def write_delta(before, after, output):
    after_bytes = after.stat().st_size
    before_bytes = before.stat().st_size
    changed_pages = 0
    with before.open("rb") as old, after.open("rb") as new, output.open("wb") as delta:
        size = sqlite_page_size(new)
        if sqlite_page_size(old) != size or before_bytes % size or after_bytes % size:
            raise ValueError("Snapshots must have the same page size and contain whole pages")
        delta.write(HEADER.pack(MAGIC, size, after_bytes))
        block_size = max(1024 * 1024, size)
        for start in range(0, after_bytes, block_size):
            previous = old.read(block_size)
            current = new.read(block_size)
            if previous == current:
                continue
            for offset in range(0, len(current), size):
                page = current[offset:offset + size]
                if previous[offset:offset + size] != page:
                    delta.write(PAGE_NUMBER.pack((start + offset) // size))
                    delta.write(page)
                    changed_pages += 1
    return size, changed_pages


def replay_delta(before, patch, restored):
    shutil.copyfile(before, restored)
    with patch.open("rb") as delta, restored.open("r+b") as output:
        magic, size, total_bytes = HEADER.unpack(delta.read(HEADER.size))
        if magic != MAGIC or size != sqlite_page_size(output) or total_bytes % size:
            raise ValueError("Delta header does not match the SQLite backup")
        output.truncate(total_bytes)
        previous = -1
        while location := delta.read(PAGE_NUMBER.size):
            page_number, = PAGE_NUMBER.unpack(location)
            position = page_number * size
            page = delta.read(size)
            if page_number <= previous or position >= total_bytes or len(page) != size:
                raise ValueError("Invalid delta page sequence")
            output.seek(position)
            output.write(page)
            previous = page_number


def measure(before, after, scratch):
    patch = scratch / "snapshot.delta"
    restored = scratch / "restored.db"
    started = time.monotonic()
    page_size, pages = write_delta(before, after, patch)
    written = time.monotonic()
    replay_delta(before, patch, restored)
    replayed = time.monotonic()
    if not filecmp.cmp(after, restored, shallow=False):
        raise ValueError("Delta replay does not reproduce the committed backup")
    return {
        "before_bytes": before.stat().st_size,
        "snapshot_bytes": after.stat().st_size,
        "page_size": page_size,
        "changed_pages": pages,
        "delta_bytes": patch.stat().st_size,
        "encode_seconds": round(written - started, 3),
        "copy_and_replay_seconds": round(replayed - written, 3),
        "verified_byte_for_byte": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before", type=Path)
    parser.add_argument("after", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="sanctum-snapshot-delta-") as directory:
        print(json.dumps(measure(args.before, args.after, Path(directory)), indent=2))


if __name__ == "__main__":
    main()
