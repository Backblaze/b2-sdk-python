######################################################################
#
# File: test/unit/bucket/test_notification_rule_tracing.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import logging

import pytest


@pytest.mark.parametrize('method_name', ['set_notification_rules', 'get_notification_rules'])
def test_notification_rule_values_are_not_traced(bucket, caplog, method_name):
    signing_value = '0123456789abcdefghijklmnopqrstuv'
    rules = [
        {
            'eventTypes': ['b2:ObjectCreated:*'],
            'isEnabled': True,
            'name': 'test-rule',
            'objectNamePrefix': '',
            'targetConfiguration': {
                'customHeaders': [],
                'targetType': 'webhook',
                'url': 'https://example.com/webhook',
                'hmacSha256SigningSecret': signing_value,
            },
        }
    ]
    if method_name == 'get_notification_rules':
        bucket.set_notification_rules(rules)

    logger = logging.getLogger('b2sdk._internal.bucket')
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        logger.debug('notification rule logging capture')
        if method_name == 'set_notification_rules':
            result = bucket.set_notification_rules(rules)
        else:
            result = bucket.get_notification_rules()

    assert (logger.name, logging.DEBUG, 'notification rule logging capture') in caplog.record_tuples
    assert result[0]['targetConfiguration']['hmacSha256SigningSecret'] == signing_value
    assert all(signing_value not in record.getMessage() for record in caplog.records)
