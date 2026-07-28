import json
import os
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from contextlib import ExitStack
from itertools import islice, product
from pathlib import Path

import numpy as np
from cloudvolume import CloudVolume
from tqdm import tqdm

from .vbc_pure import decode_vbc
from .vsv import VSV2Lookup, parse_brick_page, read_vsv2_header

_worker = None
_upload_worker = threading.local()


def _initialize_worker(source):
    global _worker
    _worker = (source, VSV2Lookup(source))


def completed_chunks(checkpoint, job):
    checkpoint = Path(checkpoint)
    if not checkpoint.exists() or checkpoint.stat().st_size == 0:
        return set()
    records = [json.loads(line) for line in checkpoint.read_text().splitlines() if line]
    if records[0] != {"job": job}:
        raise ValueError("checkpoint belongs to a different conversion job")
    return {tuple(record["chunk"]) for record in records[1:]}


def chunk_boxes(size, chunk_size, start=(0, 0, 0), shape=None):
    end = size if shape is None else tuple(min(size[i], start[i] + shape[i]) for i in range(3))
    if any(start[i] % chunk_size[i] for i in range(3)):
        raise ValueError("start must be aligned to chunk size")
    for z, y, x in product(
        range(start[2], end[2], chunk_size[2]),
        range(start[1], end[1], chunk_size[1]),
        range(start[0], end[0], chunk_size[0]),
    ):
        yield (x, y, z), tuple(min(end[i], (x, y, z)[i] + chunk_size[i]) for i in range(3))


def _decode_chunk(arguments):
    start, end = arguments
    source, lookup = _worker
    brick_size = 16
    output = np.zeros(tuple(end[i] - start[i] for i in range(3)), dtype=np.uint8)
    pages = {}
    with open(source, "rb") as stream:
        for bz in range(start[2] // brick_size, (end[2] + brick_size - 1) // brick_size):
            for by in range(start[1] // brick_size, (end[1] + brick_size - 1) // brick_size):
                for bx in range(start[0] // brick_size, (end[0] + brick_size - 1) // brick_size):
                    page_offset, child = lookup.locate(bx, by, bz)
                    if not page_offset:
                        continue
                    if page_offset not in pages:
                        stream.seek(page_offset)
                        pages[page_offset] = parse_brick_page(stream.read(40960))
                    lengths, offsets = pages[page_offset]
                    length, offset = lengths[child], offsets[child]
                    if not length:
                        brick = bytes(brick_size ** 3)
                    else:
                        stream.seek(offset)
                        payload = stream.read(length)
                        if payload[0] == 0:
                            brick = payload[1:]
                        elif payload[0] == 1:
                            brick = payload[1:2] * brick_size ** 3
                        elif payload[0] == 2:
                            brick = decode_vbc(payload)
                        else:
                            raise ValueError(f"unsupported VAST brick mode {payload[0]}")
                    if len(brick) != brick_size ** 3:
                        raise ValueError(f"decoded brick has {len(brick)} bytes, expected {brick_size ** 3}")
                    array = np.frombuffer(brick, dtype=np.uint8).reshape((16, 16, 16), order="F")
                    origin = (bx * 16, by * 16, bz * 16)
                    src = tuple(slice(max(0, start[i] - origin[i]), min(16, end[i] - origin[i])) for i in range(3))
                    dst = tuple(slice(origin[i] + src[i].start - start[i], origin[i] + src[i].stop - start[i]) for i in range(3))
                    output[dst] = array[src]
    return start, end, output


def _upload_chunk(destination, start, end, data, retries):
    volume = getattr(_upload_worker, "volume", None)
    if volume is None or _upload_worker.destination != destination:
        volume = CloudVolume(destination, mip=0, compress=True, parallel=False, progress=False)
        _upload_worker.destination = destination
        _upload_worker.volume = volume
    slices = tuple(slice(start[i], end[i]) for i in range(3))
    for attempt in range(retries):
        try:
            volume[slices] = data[..., None]
            return
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def _checkpoint_uploads(pending, log, progress):
    done, _ = wait(pending, return_when=FIRST_COMPLETED)
    error = None
    for future in done:
        box_start = pending.pop(future)
        try:
            future.result()
        except Exception as exception:
            error = error or exception
            continue
        log.write(json.dumps({"chunk": box_start}) + "\n")
        log.flush()
        progress.update()
    if error:
        for future in pending:
            future.cancel()
        raise error


def convert_vsv(
    source,
    destination,
    chunk_size=(512, 512, 16),
    workers=4,
    upload_workers=1,
    retries=5,
    checkpoint=None,
    profile=None,
    start=(0, 0, 0),
    shape=None,
):
    source = str(Path(source).resolve())
    with open(source, "rb") as stream:
        header = read_vsv2_header(stream)
    if header.bytes_per_voxel != 1 or header.brick_size != (16, 16, 16):
        raise ValueError("converter currently supports uint8 VSV2 volumes with 16x16x16 bricks")
    if any(value % 16 for value in chunk_size):
        raise ValueError("chunk size must be a multiple of 16")
    if workers < 1 or upload_workers < 1 or retries < 1:
        raise ValueError("workers, upload workers, and retries must be positive")
    if profile:
        os.environ["AWS_PROFILE"] = profile
    info = CloudVolume.create_new_info(
        num_channels=1,
        layer_type="image",
        data_type="uint8",
        encoding="raw",
        resolution=[round(value, 6) for value in header.voxel_size_nm],
        voxel_offset=[0, 0, 0],
        volume_size=list(header.size),
        chunk_size=list(chunk_size),
        max_mip=0,
    )
    volume = CloudVolume(destination, info=info, compress=True, parallel=False, progress=False)
    volume.commit_info()
    checkpoint = Path(checkpoint or f"{Path(source).stem}.checkpoint.jsonl")
    job = {
        "source": source,
        "destination": destination,
        "chunk_size": list(chunk_size),
        "start": list(start),
        "shape": None if shape is None else list(shape),
    }
    completed = completed_chunks(checkpoint, job)
    boxes = [(box_start, box_end) for box_start, box_end in chunk_boxes(header.size, chunk_size, start, shape) if box_start not in completed]
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    new_checkpoint = not checkpoint.exists() or checkpoint.stat().st_size == 0
    decode_batch_size = workers * 2
    upload_buffer_size = upload_workers * 2
    arguments = iter(boxes)
    with ExitStack() as stack:
        pool = stack.enter_context(
            ProcessPoolExecutor(
                max_workers=workers,
                initializer=_initialize_worker,
                initargs=(source,),
            )
        )
        upload_pool = stack.enter_context(ThreadPoolExecutor(max_workers=upload_workers))
        log = stack.enter_context(checkpoint.open("a"))
        progress = stack.enter_context(tqdm(total=len(boxes), desc="Convert"))
        if new_checkpoint:
            log.write(json.dumps({"job": job}) + "\n")
            log.flush()
        pending = {}
        while batch := list(islice(arguments, decode_batch_size)):
            for box_start, box_end, data in pool.map(_decode_chunk, batch):
                while len(pending) >= upload_buffer_size:
                    _checkpoint_uploads(pending, log, progress)
                future = upload_pool.submit(
                    _upload_chunk, destination, box_start, box_end, data, retries
                )
                pending[future] = box_start
        while pending:
            _checkpoint_uploads(pending, log, progress)
