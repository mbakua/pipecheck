"""Tests for pipecheck.annotations."""

import pytest
from pathlib import Path

from pipecheck.annotations import (
    Annotation,
    add_annotation,
    get_annotations,
    delete_annotations,
    init_annotations_db,
)


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "test.db"
    init_annotations_db(db)
    return db


def test_add_and_get_annotation(tmp_db: Path) -> None:
    ann = add_annotation("pipe-a", "looks healthy", author="alice", db_path=tmp_db)
    assert isinstance(ann, Annotation)
    assert ann.pipeline == "pipe-a"
    assert ann.note == "looks healthy"
    assert ann.author == "alice"
    assert ann.created_at  # non-empty ISO string


def test_get_annotations_returns_newest_first(tmp_db: Path) -> None:
    add_annotation("pipe-b", "first note", db_path=tmp_db)
    add_annotation("pipe-b", "second note", db_path=tmp_db)
    results = get_annotations("pipe-b", db_path=tmp_db)
    assert len(results) == 2
    assert results[0].note == "second note"
    assert results[1].note == "first note"


def test_get_annotations_empty_for_unknown_pipeline(tmp_db: Path) -> None:
    results = get_annotations("nonexistent", db_path=tmp_db)
    assert results == []


def test_get_annotations_limit(tmp_db: Path) -> None:
    for i in range(10):
        add_annotation("pipe-c", f"note {i}", db_path=tmp_db)
    results = get_annotations("pipe-c", limit=3, db_path=tmp_db)
    assert len(results) == 3


def test_annotations_isolated_by_pipeline(tmp_db: Path) -> None:
    add_annotation("pipe-x", "note for x", db_path=tmp_db)
    add_annotation("pipe-y", "note for y", db_path=tmp_db)
    x_results = get_annotations("pipe-x", db_path=tmp_db)
    assert len(x_results) == 1
    assert x_results[0].pipeline == "pipe-x"


def test_delete_annotations_removes_all(tmp_db: Path) -> None:
    add_annotation("pipe-d", "note 1", db_path=tmp_db)
    add_annotation("pipe-d", "note 2", db_path=tmp_db)
    deleted = delete_annotations("pipe-d", db_path=tmp_db)
    assert deleted == 2
    assert get_annotations("pipe-d", db_path=tmp_db) == []


def test_delete_annotations_nonexistent_pipeline(tmp_db: Path) -> None:
    deleted = delete_annotations("ghost-pipe", db_path=tmp_db)
    assert deleted == 0


def test_default_author_is_empty_string(tmp_db: Path) -> None:
    add_annotation("pipe-e", "anonymous note", db_path=tmp_db)
    results = get_annotations("pipe-e", db_path=tmp_db)
    assert results[0].author == ""
