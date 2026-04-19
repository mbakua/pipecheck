"""Tag-based filtering for pipelines."""
from __future__ import annotations
from typing import List, Optional
from pipecheck.config import PipelineConfig


def parse_tags(tag_string: Optional[str]) -> List[str]:
    """Parse a comma-separated tag string into a list of tags."""
    if not tag_string:
        return []
    return [t.strip() for t in tag_string.split(",") if t.strip()]


def pipeline_has_tags(pipeline: PipelineConfig, tags: List[str]) -> bool:
    """Return True if the pipeline has ALL of the given tags."""
    if not tags:
        return True
    pipeline_tags = getattr(pipeline, "tags", None) or []
    return all(t in pipeline_tags for t in tags)


def filter_pipelines(
    pipelines: List[PipelineConfig],
    include_tags: Optional[List[str]] = None,
    exclude_tags: Optional[List[str]] = None,
) -> List[PipelineConfig]:
    """Filter pipelines by included and excluded tags."""
    result = []
    for p in pipelines:
        p_tags = getattr(p, "tags", None) or []
        if include_tags and not all(t in p_tags for t in include_tags):
            continue
        if exclude_tags and any(t in p_tags for t in exclude_tags):
            continue
        result.append(p)
    return result
