"""Writer layout rules — pure, no Spark session needed."""

import pytest

from dev_platform.core.common.writers import _normalize_layout


def test_layout_defaults_empty():
    assert _normalize_layout(None, None) == ([], [])


def test_layout_partition_only():
    assert _normalize_layout(["dt"], None) == (["dt"], [])


def test_layout_cluster_only():
    assert _normalize_layout(None, ["customerid"]) == ([], ["customerid"])


def test_layout_partition_and_cluster_conflict():
    with pytest.raises(ValueError, match="mutually exclusive"):
        _normalize_layout(["dt"], ["customerid"])
