# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Distinguish invalid transaction input from per-item cancellation reasons.

Every failed request includes an earlier valid put and verifies it rolled back.
This prevents correct error text from concealing partial transaction commits.
"""
import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def indexed_table(create_and_cleanup_table):
    return create_and_cleanup_table(
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                              {"AttributeName": "g", "AttributeType": "S"}],
        GlobalSecondaryIndexes=[{"IndexName": "by_g", "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                                 "Projection": {"ProjectionType": "ALL"}}],
    )["TableDescription"]["TableName"]


@pytest.mark.parametrize("operation", ["Delete", "Update", "ConditionCheck"])
@pytest.mark.parametrize("bad_key", [{"N": "123"}, {"L": []}])
def test_invalid_key_cancels_without_committing(dynamodb_client, indexed_table, operation, bad_key):
    client, name = dynamodb_client, indexed_table
    action = {"TableName": name, "Key": {"pk": bad_key}}
    if operation == "Update":
        action.update(UpdateExpression="SET value_attr = :v", ExpressionAttributeValues={":v": {"S": "value"}})
    elif operation == "ConditionCheck":
        action.update(ConditionExpression="attribute_not_exists(pk)")
    with pytest.raises(ClientError) as error:
        client.transact_write_items(TransactItems=[
            {"Put": {"TableName": name, "Item": {"pk": {"S": "earlier"}}}},
            {operation: action},
        ])
    assert error.value.response["Error"]["Code"] == "TransactionCanceledException"
    reasons = error.value.response["CancellationReasons"]
    assert [r["Code"] for r in reasons] == ["None", "ValidationError"]
    assert "does not match the schema" in reasons[1]["Message"]
    assert "Item" not in client.get_item(TableName=name, Key={"pk": {"S": "earlier"}}, ConsistentRead=True)


@pytest.mark.parametrize("operation", ["Put", "Update"])
def test_empty_index_key_is_request_validation(dynamodb_client, indexed_table, operation):
    client, name = dynamodb_client, indexed_table
    action = {"TableName": name}
    if operation == "Put":
        action["Item"] = {"pk": {"S": "bad"}, "g": {"S": ""}}
    else:
        action.update(Key={"pk": {"S": "bad"}}, UpdateExpression="SET g = :g", ExpressionAttributeValues={":g": {"S": ""}})
    with pytest.raises(ClientError) as error:
        client.transact_write_items(TransactItems=[
            {"Put": {"TableName": name, "Item": {"pk": {"S": "earlier"}}}}, {operation: action},
        ])
    assert error.value.response["Error"]["Code"] == "ValidationException"
    assert "empty" in error.value.response["Error"]["Message"].lower()
    for key in ["earlier", "bad"]:
        assert "Item" not in client.get_item(TableName=name, Key={"pk": {"S": key}}, ConsistentRead=True)
