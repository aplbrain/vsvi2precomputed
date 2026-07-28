import json
from concurrent.futures import Future
from io import StringIO
from unittest.mock import Mock

import numpy as np
import pytest

from vsvi2precomputed.converter import (
    _checkpoint_uploads,
    _decode_chunk,
    _upload_chunk,
    chunk_boxes,
    completed_chunks,
)
import vsvi2precomputed.converter as converter


def test_chunk_boxes_include_cropped_edges():
    assert list(chunk_boxes((10, 9, 5), (4, 4, 4))) == [
        ((0, 0, 0), (4, 4, 4)), ((4, 0, 0), (8, 4, 4)), ((8, 0, 0), (10, 4, 4)),
        ((0, 4, 0), (4, 8, 4)), ((4, 4, 0), (8, 8, 4)), ((8, 4, 0), (10, 8, 4)),
        ((0, 8, 0), (4, 9, 4)), ((4, 8, 0), (8, 9, 4)), ((8, 8, 0), (10, 9, 4)),
        ((0, 0, 4), (4, 4, 5)), ((4, 0, 4), (8, 4, 5)), ((8, 0, 4), (10, 4, 5)),
        ((0, 4, 4), (4, 8, 5)), ((4, 4, 4), (8, 8, 5)), ((8, 4, 4), (10, 8, 5)),
        ((0, 8, 4), (4, 9, 5)), ((4, 8, 4), (8, 9, 5)), ((8, 8, 4), (10, 9, 5)),
    ]


def test_chunk_boxes_limit_region():
    assert list(chunk_boxes((100, 100, 100), (16, 16, 16), (32, 48, 64), (16, 32, 16))) == [
        ((32, 48, 64), (48, 64, 80)),
        ((32, 64, 64), (48, 80, 80)),
    ]


def test_chunk_boxes_require_aligned_start():
    with pytest.raises(ValueError, match="aligned"):
        list(chunk_boxes((100, 100, 100), (16, 16, 16), (1, 0, 0)))


def test_completed_chunks_rejects_different_job(tmp_path):
    checkpoint = tmp_path / "checkpoint.jsonl"
    checkpoint.write_text(json.dumps({"job": {"source": "a"}}) + "\n")
    with pytest.raises(ValueError, match="different conversion job"):
        completed_chunks(checkpoint, {"source": "b"})


def test_completed_chunks_reads_chunk_records(tmp_path):
    job = {"source": "a"}
    checkpoint = tmp_path / "checkpoint.jsonl"
    checkpoint.write_text("\n".join((json.dumps({"job": job}), json.dumps({"chunk": [1, 2, 3]}))) + "\n")
    assert completed_chunks(checkpoint, job) == {(1, 2, 3)}


def test_decode_chunk_skips_sparse_pages(monkeypatch, tmp_path):
    source = tmp_path / "sample.vsv"
    source.write_bytes(b"VSV2")

    class SparseLookup:
        def locate(self, *coordinate):
            return 0, 0

    monkeypatch.setattr(converter, "_worker", (str(source), SparseLookup()))
    _, _, data = _decode_chunk(((0, 0, 0), (16, 16, 16)))

    assert not data.any()


def test_upload_chunk_retries(monkeypatch):
    writes = []

    class FakeVolume:
        def __setitem__(self, slices, data):
            writes.append((slices, data.shape))
            if len(writes) < 3:
                raise OSError("temporary failure")

    monkeypatch.setattr(converter, "CloudVolume", lambda *args, **kwargs: FakeVolume())
    monkeypatch.setattr(converter.time, "sleep", lambda delay: None)
    monkeypatch.setattr(converter._upload_worker, "volume", None, raising=False)

    _upload_chunk("file:///output", (0, 0, 0), (16, 16, 16), np.zeros((16, 16, 16)), 3)

    assert len(writes) == 3
    assert writes[-1][1] == (16, 16, 16, 1)


def test_checkpoint_uploads_records_only_successes():
    success = Future()
    success.set_result(None)
    failure = Future()
    failure.set_exception(OSError("upload failed"))
    pending = {success: (1, 2, 3), failure: (4, 5, 6)}
    log = StringIO()
    progress = Mock()

    with pytest.raises(OSError, match="upload failed"):
        _checkpoint_uploads(pending, log, progress)

    assert json.loads(log.getvalue()) == {"chunk": [1, 2, 3]}
    progress.update.assert_called_once_with()
