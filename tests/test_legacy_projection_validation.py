# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Legacy duplicate checks are independent of item existence and projection paths."""
import pytest
from botocore.exceptions import ClientError


def read(client, operation, name, attributes):
    key = {"pk": {"S": "key"}}
    if operation == "batch_get_item":
        return client.batch_get_item(RequestItems={name: {"Keys": [key], "AttributesToGet": attributes}})
    args = dict(TableName=name, AttributesToGet=attributes, ConsistentRead=True)
    if operation == "get_item":
        args["Key"] = key
    if operation == "query":
        args["KeyConditions"] = {"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [key["pk"]]}}
    return getattr(client, operation)(**args)


@pytest.mark.parametrize("operation", ["get_item", "query", "scan", "batch_get_item"])
@pytest.mark.parametrize("present", [False, True])
def test_legacy_duplicate_names_rejected(dynamodb_client, create_and_cleanup_table, operation, present):
    name = create_and_cleanup_table()["TableDescription"]["TableName"]
    if present:
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "key"}, "a": {"S": "value"}})
    with pytest.raises(ClientError) as error:
        read(dynamodb_client, operation, name, ["a", "a"])
    assert error.value.response["Error"]["Code"] == "ValidationException"


@pytest.mark.parametrize("operation", ["get_item", "query", "scan", "batch_get_item"])
def test_legacy_names_are_literal_not_paths(dynamodb_client, create_and_cleanup_table, operation):
    name = create_and_cleanup_table()["TableDescription"]["TableName"]
    attributes = {"a": {"S": "root"}, "a.b": {"S": "dot"}, "a[0]": {"S": "bracket"},
                  "名字": {"S": "unicode"}}
    dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "key"}, **attributes})
    response = read(dynamodb_client, operation, name, list(attributes))
    if operation == "get_item":
        assert response["Item"] == attributes
    elif operation == "batch_get_item":
        assert response["Responses"][name] == [attributes]
    else:
        assert response["Items"] == [attributes]
