"""Reliable lakehouse pipeline reference implementation."""

from .merge import MergeStats, merge_batch

__all__ = ["MergeStats", "merge_batch"]
