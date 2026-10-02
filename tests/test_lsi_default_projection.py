# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""LSI defaults, explicit selection, and base-table projection reachback.

The same assertions run on AWS and each backend. In particular, fetching a
base image internally must not expose unprojected fields by default.
"""
import pytest
from conftest import scoped_table


@pytest.mark.parametrize("operation", ["query", "scan"])
@pytest.mark.parametrize("selection, expected", [
    ({}, {"pk", "sk", "rank", "included"}),
    ({"Select": "ALL_PROJECTED_ATTRIBUTES"}, {"pk", "sk", "rank", "included"}),
    ({"Select": "ALL_ATTRIBUTES"}, {"pk", "sk", "rank", "included", "payload"}),
    ({"ProjectionExpression": "payload"}, {"payload"}),
])
def test_lsi_projection_modes(dynamodb_client, operation, selection, expected):
    client = dynamodb_client
    with scoped_table(
        client,
        attribute_definitions=[
            {"AttributeName": name, "AttributeType": "S"}
            for name in ("pk", "sk", "rank")
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        LocalSecondaryIndexes=[{
            "IndexName": "rank-index",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "rank", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["included"]},
        }],
    ) as table:
        item = {name: {"S": name} for name in ("pk", "sk", "rank", "included", "payload")}
        client.put_item(TableName=table, Item=item)
        request = dict(TableName=table, IndexName="rank-index", ConsistentRead=True, **selection)
        if operation == "query":
            request.update(KeyConditionExpression="pk = :pk", ExpressionAttributeValues={":pk": item["pk"]})
        response = getattr(client, operation)(**request)
        assert response["Items"] == [{key: item[key] for key in expected}]
