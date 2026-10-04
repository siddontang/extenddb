# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Regression contracts for validation before reads and atomic updates.

Invalid syntax is checked on both empty and populated items; valid expressions
then verify list ordering and nested set mutation against the original image.
"""
import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def table(create_and_cleanup_table):
    return create_and_cleanup_table()["TableDescription"]["TableName"]


@pytest.mark.parametrize("expression", [
    "SET a = :v SET b = :v", "SET a = :v REMOVE a",
    "SET a.b = :v, a[1] = :v", "SET a = :v, a.b = :v",
    "SET a = if_not_exists(:v, :v)", "SET a = LIST_APPEND(:v, :v)",
    "SET a = :v, #alias = :v", "SET _invalid = :v",
])
def test_invalid_update_preserves_image(dynamodb_client, table, expression):
    client = dynamodb_client
    original = {"pk": {"S": "key"}, "a": {"S": "before"}}
    client.put_item(TableName=table, Item=original)
    params = dict(TableName=table, Key={"pk": original["pk"]}, UpdateExpression=expression,
                  ExpressionAttributeValues={":v": {"S": "after"}})
    if "#alias" in expression:
        params["ExpressionAttributeNames"] = {"#alias": "a"}
    with pytest.raises(ClientError, match="ValidationException"):
        client.update_item(**params)
    assert client.get_item(TableName=table, Key={"pk": original["pk"]}, ConsistentRead=True)["Item"] == original


@pytest.mark.parametrize("condition", [
    "", "attribute_exists(:v)", "attribute_type(absent, :v)",
    "attribute_type(absent, other)", "size(size(absent)) = :v",
    "attribute_exists(pk) OR attribute_type(absent, :v)",
])
def test_invalid_condition_cannot_be_hidden_by_absence_or_short_circuit(dynamodb_client, table, condition):
    client = dynamodb_client
    key = {"pk": {"S": "key"}}
    for exists in [False, True]:
        if exists:
            client.put_item(TableName=table, Item=key)
        params = dict(TableName=table, Key=key, UpdateExpression="SET changed = :v",
                      ConditionExpression=condition, ExpressionAttributeValues={":v": {"S": "DOG"}})
        with pytest.raises(ClientError, match="ValidationException"):
            client.update_item(**params)
        assert client.get_item(TableName=table, Key=key, ConsistentRead=True).get("Item") == (key if exists else None)


@pytest.mark.parametrize("projection", ["a.b, a[0]", "a" + ".b" * 32])
def test_invalid_projection_on_missing_item(dynamodb_client, table, projection):
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb_client.get_item(TableName=table, Key={"pk": {"S": "absent"}}, ProjectionExpression=projection)


def test_list_positions_and_nested_set_actions(dynamodb_client, table):
    client = dynamodb_client
    key = {"pk": {"S": "key"}}
    client.put_item(TableName=table, Item={**key, "a": {"L": [{"S": "old"}]}, "nested": {"L": [{"SS": ["one"]}]}})
    client.update_item(TableName=table, Key=key, UpdateExpression="SET a[20] = :high, a[10] = :low ADD nested[0] :set",
                       ExpressionAttributeValues={":high": {"S": "high"}, ":low": {"S": "low"}, ":set": {"SS": ["two"]}})
    item = client.get_item(TableName=table, Key=key, ConsistentRead=True)["Item"]
    assert item["a"] == {"L": [{"S": "old"}, {"S": "low"}, {"S": "high"}]}
    assert set(item["nested"]["L"][0]["SS"]) == {"one", "two"}
    client.update_item(TableName=table, Key=key, UpdateExpression="REMOVE a[0], a[1]")
    assert client.get_item(TableName=table, Key=key, ConsistentRead=True)["Item"]["a"] == {"L": [{"S": "high"}]}


@pytest.mark.parametrize("update", [
    {"UpdateExpression": "REMOVE absent"},
    {"UpdateExpression": "REMOVE absent.child"},
    {"UpdateExpression": "DELETE absent :v", "ExpressionAttributeValues": {":v": {"SS": ["value"]}}},
    {"AttributeUpdates": {"absent": {"Action": "DELETE"}}},
])
def test_delete_only_update_does_not_create_item(dynamodb_client, table, update):
    client = dynamodb_client
    key = {"pk": {"S": "missing"}}
    result = client.update_item(TableName=table, Key=key, ReturnValues="ALL_NEW", **update)
    assert not result.get("Attributes")
    assert "Item" not in client.get_item(TableName=table, Key=key, ConsistentRead=True)


@pytest.mark.parametrize("expected", [
    {"Exists": True}, {"ComparisonOperator": "NULL", "AttributeValueList": [{"S": "value"}]},
    {"ComparisonOperator": "IN", "AttributeValueList": [{"S": "1"}, {"N": "1"}]},
    {"ComparisonOperator": "LE", "AttributeValueList": [{"L": []}]},
    {"ComparisonOperator": "BETWEEN", "AttributeValueList": [{"N": "10"}, {"N": "2"}]},
])
def test_legacy_expected_validated_before_missing_item(dynamodb_client, table, expected):
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "missing"}}, Expected={"absent": expected})
    assert "Item" not in dynamodb_client.get_item(TableName=table, Key={"pk": {"S": "missing"}}, ConsistentRead=True)


@pytest.mark.parametrize("number", ["1e126", "1e-131", "1.123456789012345678901234567890123456789", "1.1e-2147483648"])
def test_numeric_keys_enforce_number_bounds(dynamodb_client, create_and_cleanup_table, number):
    table = create_and_cleanup_table(AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "N"}])["TableDescription"]["TableName"]
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb_client.update_item(TableName=table, Key={"pk": {"N": number}}, UpdateExpression="SET value_attr = :v", ExpressionAttributeValues={":v": {"S": "value"}})
    assert dynamodb_client.scan(TableName=table, ConsistentRead=True)["Count"] == 0


def test_condition_functions_accept_constant_values(dynamodb_client, table):
    client = dynamodb_client
    key = {"pk": {"S": "constants"}}
    client.put_item(TableName=table, Item=key, ConditionExpression="attribute_type(:text, :kind) AND size(:text) = :length",
                    ExpressionAttributeValues={":text": {"S": "hello"}, ":kind": {"S": "S"}, ":length": {"N": "5"}})
    assert client.get_item(TableName=table, Key=key, ConsistentRead=True)["Item"] == key
    with pytest.raises(ClientError, match="ValidationException"):
        client.put_item(TableName=table, Item=key, ConditionExpression="attribute_exists(pk) OR size(:number) = :number",
                        ExpressionAttributeValues={":number": {"N": "5"}})


def test_legacy_list_append_and_missing_not_contains(dynamodb_client, table):
    client = dynamodb_client
    key = {"pk": {"S": "legacy"}}
    for value in [{"L": [{"N": "1"}]}, {"L": [{"S": "two"}]}]:
        client.update_item(TableName=table, Key=key, AttributeUpdates={"history": {"Action": "ADD", "Value": value}})
    original = {**key, "history": {"L": [{"N": "1"}, {"S": "two"}]}}
    assert client.get_item(TableName=table, Key=key, ConsistentRead=True)["Item"] == original
    with pytest.raises(ClientError, match="ConditionalCheckFailedException"):
        client.put_item(TableName=table, Item=key, Expected={"absent": {"ComparisonOperator": "NOT_CONTAINS", "AttributeValueList": [{"S": "value"}]}})
    assert client.get_item(TableName=table, Key=key, ConsistentRead=True)["Item"] == original
    # Modern ADD has a different contract and must still reject lists.
    with pytest.raises(ClientError, match="ValidationException"):
        client.update_item(TableName=table, Key=key, UpdateExpression="ADD history :value", ExpressionAttributeValues={":value": {"L": []}})


@pytest.mark.parametrize("operation", ["query", "scan"])
@pytest.mark.parametrize("select", ["COUNT", "ALL_ATTRIBUTES"])
def test_legacy_projection_conflicts_with_select(dynamodb_client, table, operation, select):
    params = dict(TableName=table, Select=select, AttributesToGet=["value"])
    if operation == "query":
        params["KeyConditions"] = {"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "absent"}]}}
    with pytest.raises(ClientError, match="ValidationException.*AttributesToGet"):
        getattr(dynamodb_client, operation)(**params)


def test_query_conditions_are_independent_of_input_order(dynamodb_client, create_and_cleanup_table):
    table = create_and_cleanup_table(
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "sk", "AttributeType": "N"}, {"AttributeName": "pk", "AttributeType": "S"}],
    )["TableDescription"]["TableName"]
    client = dynamodb_client
    item = {"pk": {"S": "partition"}, "sk": {"N": "3"}}
    client.put_item(TableName=table, Item=item)
    result = client.query(TableName=table, KeyConditions={"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "partition"}]}})
    assert result["Items"] == [item]
    reverse = client.query(TableName=table, KeyConditionExpression="sk = :s AND pk = :p",
                           ExpressionAttributeValues={":s": item["sk"], ":p": item["pk"]})
    assert reverse["Items"] == [item]
