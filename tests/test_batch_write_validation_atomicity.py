# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Invalid batches must be rejected before any valid put or delete executes.

These assertions cover request validation, not runtime throttling: DynamoDB
may partially process a valid batch, but an invalid schema/item rejects it.
The tests deliberately put a valid overwrite and deletion before the bad row.
"""
import pytest
from botocore.exceptions import ClientError
from conftest import wait_for_active


@pytest.fixture
def indexed_batch_table(dynamodb_client, unique_table_name):
    name = unique_table_name
    dynamodb_client.create_table(
        TableName=name, BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "by_g", "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
    )
    try:
        wait_for_active(dynamodb_client, name)
        yield name
    finally:
        dynamodb_client.delete_table(TableName=name)


def put(item):
    return {"PutRequest": {"Item": item}}


@pytest.mark.parametrize("bad", [
    put({"pk": {"S": "bad"}, "g": {"N": "1"}}),
    put({"pk": {"S": "bad"}, "g": {"S": ""}}),
    put({"pk": {"S": "bad"}, "g": {"L": []}}),
    put({"g": {"S": "missing_pk"}}),
    {"DeleteRequest": {"Key": {"pk": {"N": "1"}}}},
    put({"pk": {"S": "bad"}, "large": {"S": "x" * (400 * 1024)}}),
])
def test_invalid_later_operation_preserves_earlier_items(
    dynamodb_client, indexed_batch_table, bad
):
    name = indexed_batch_table
    originals = [{"pk": {"S": k}, "value": {"S": "before"}} for k in ("overwrite", "delete")]
    for item in originals:
        dynamodb_client.put_item(TableName=name, Item=item)
    with pytest.raises(ClientError) as error:
        dynamodb_client.batch_write_item(RequestItems={name: [
            put({"pk": {"S": "overwrite"}, "value": {"S": "after"}}),
            {"DeleteRequest": {"Key": {"pk": {"S": "delete"}}}},
            bad,
        ]})
    assert error.value.response["Error"]["Code"] == "ValidationException"
    for item in originals:
        assert dynamodb_client.get_item(
            TableName=name, Key={"pk": item["pk"]}, ConsistentRead=True,
        )["Item"] == item


def test_missing_table_rejects_other_tables_before_writing(
    dynamodb_client, indexed_batch_table
):
    name = indexed_batch_table
    # Repeated requests exercise the server's arbitrary map iteration order.
    for attempt in range(8):
        key = {"pk": {"S": str(attempt)}}
        with pytest.raises(ClientError) as error:
            dynamodb_client.batch_write_item(RequestItems={
                name: [put(key)], name + "_missing": [put(key)],
            })
        assert error.value.response["Error"]["Code"] == "ResourceNotFoundException"
        assert "Item" not in dynamodb_client.get_item(
            TableName=name, Key=key, ConsistentRead=True,
        )
