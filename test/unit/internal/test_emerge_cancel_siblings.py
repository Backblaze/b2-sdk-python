######################################################################
#
# File: test/unit/internal/test_emerge_cancel_siblings.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from b2sdk._internal.transfer.emerge.executor import LargeFileEmergeExecution


def test_failed_part_cancels_pending_siblings_and_keeps_unfinished_upload():
    services = MagicMock()
    execution = LargeFileEmergeExecution(services, 'bucket', 'name', None, {}, MagicMock())
    execution._get_unfinished_file_and_parts = MagicMock(
        return_value=(SimpleNamespace(file_id='unfinished'), {})
    )
    plan = MagicMock()
    plan.get_total_length.return_value = None
    plan.get_plan_id.return_value = None
    plan.enumerate_emerge_parts.return_value = iter([(1, object()), (2, object()), (3, object())])
    first, failed, pending = Future(), Future(), Future()
    first.set_result({'contentSha1': 'one'})
    steps = [MagicMock(), MagicMock(), MagicMock()]
    for step, future in zip(steps, (first, failed, pending)):
        step.execute.return_value = future
    steps[2].execute.side_effect = lambda: (
        failed.set_exception(ValueError('part failed')),
        pending,
    )[1]
    with patch(
        'b2sdk._internal.transfer.emerge.executor.LargeFileEmergeExecutionStepFactory'
    ) as factory:
        factory.side_effect = [
            SimpleNamespace(get_execution_step=lambda step=step: step) for step in steps
        ]
        with pytest.raises(ValueError, match='part failed'):
            execution.execute_plan(plan)
    assert pending.cancelled()
    services.session.finish_large_file.assert_not_called()
    services.large_file.cancel_large_file.assert_not_called()
