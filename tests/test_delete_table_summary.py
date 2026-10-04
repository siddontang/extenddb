# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""DeleteTable's wire summary omits schema but retains identity and capacity.

The AWS DeleteTable example and the upstream Alternator assertions distinguish
this response from DescribeTable. The projection belongs to the shared engine,
so this contract runs unchanged against TiKV, PostgreSQL, and SQLite.
"""
import pytest
from conftest import wait_for_deleted


@pytest.mark.parametrize("indexes", [False, True])
def test_delete_summary(dynamodb_client, create_and_cleanup_table, indexes):
    options = dict(
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                              {"AttributeName": "sk", "AttributeType": "S"}],
    )
    if indexes:
        options["AttributeDefinitions"].append({"AttributeName": "g", "AttributeType": "S"})
        options["GlobalSecondaryIndexes"] = [{
            "IndexName": "global_index", "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }]
        options["LocalSecondaryIndexes"] = [{
            "IndexName": "local_index", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"},
                                                      {"AttributeName": "g", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "ALL"},
        }]
    created = create_and_cleanup_table(**options)["TableDescription"]
    name = created["TableName"]
    before = dynamodb_client.describe_table(TableName=name)["Table"]
    deleted = dynamodb_client.delete_table(TableName=name)["TableDescription"]
    assert deleted["TableStatus"] == "DELETING"
    for field in ("TableName", "TableId", "TableArn", "ProvisionedThroughput", "BillingModeSummary"):
        assert deleted[field] == before[field]
    for field in ("CreationDateTime", "KeySchema", "AttributeDefinitions",
                  "GlobalSecondaryIndexes", "LocalSecondaryIndexes"):
        assert field not in deleted
    assert "KeySchema" in before
    if indexes:
        assert "GlobalSecondaryIndexes" in before and "LocalSecondaryIndexes" in before
    wait_for_deleted(dynamodb_client, name)
