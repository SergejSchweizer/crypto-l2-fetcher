"""Tests for shared CLI command runtime helpers."""

from __future__ import annotations

import json
import logging

import pytest

from api.commands.runtime import DatasetCommandResult, emit_dataset_command_result


def test_emit_dataset_command_result_writes_json_and_summary(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify the runtime template emits command JSON and dataset summary logs."""

    logger = logging.getLogger("test_emit_dataset_command_result")
    result = DatasetCommandResult(
        command="example-builder",
        dataset_type="example_snapshot_1m",
        payload={"rows_written": 3, "status": "complete"},
        summary_fields={"rows_written": 3, "status": "complete"},
        json_output=True,
    )

    with caplog.at_level(logging.INFO, logger=logger.name):
        emit_dataset_command_result(logger, result)

    assert json.loads(capsys.readouterr().out) == {"rows_written": 3, "status": "complete"}
    assert caplog.messages == [
        "job_event command=example-builder event=run_summary "
        "dataset_type=example_snapshot_1m exchange=deribit rows_written=3 status=complete"
    ]
