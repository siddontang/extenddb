# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Byte-exact numeric item limits, shared by every storage backend.

Fixtures pin measured costs rather than deriving them with the implementation's
formula. Each write surface must accept 400 KiB, reject one byte more, and leave
the previous image intact. The update uses a long key so the finished-item limit
binds even in AWS regions with an additional statement-size limit.
"""
import pytest
from botocore.exceptions import ClientError


@pytest.mark.parametrize("literal,cost", [
    ("0", 1), ("1.5", 3), ("1.200", 3), ("1.234", 4), ("3.14159", 5),
    ("100.5", 4), ("0.15", 2), ("-42", 3), ("15e-1", 3), ("1E125", 2),
    ("-1.2345678901234567890123456789012345678", 21),
])
@pytest.mark.parametrize("surface", ["put", "batch", "update", "transaction"])
def test_numeric_item_size_boundary(dynamodb_client, create_and_cleanup_table, literal, cost, surface):
    client = dynamodb_client
    name = create_and_cleanup_table()["TableDescription"]["TableName"]
    key = {"pk": {"S": "k" * 100}}
    padding = 400 * 1024 - 2 - 100 - 1 - cost - 1

    def write(extra):
        item = {**key, "n": {"N": literal}, "p": {"S": "x" * (padding + extra)}}
        if surface == "put":
            client.put_item(TableName=name, Item=item)
        elif surface == "batch":
            result = client.batch_write_item(RequestItems={name: [{"PutRequest": {"Item": item}}]})
            assert not result.get("UnprocessedItems")
        else:
            update = dict(TableName=name, Key=key, UpdateExpression="SET n = :n, p = :p",
                          ExpressionAttributeValues={":n": item["n"], ":p": item["p"]})
            if surface == "update":
                client.update_item(**update)
            else:
                client.transact_write_items(TransactItems=[{"Update": update}])

    write(0)
    before = client.get_item(TableName=name, Key=key, ConsistentRead=True)["Item"]
    assert len(before["p"]["S"]) == padding
    with pytest.raises(ClientError) as error:
        write(1)
    if surface == "transaction":
        assert error.value.response["Error"]["Code"] == "TransactionCanceledException"
        assert error.value.response["CancellationReasons"][0]["Code"] == "ValidationError"
    else:
        assert error.value.response["Error"]["Code"] == "ValidationException"
    assert client.get_item(TableName=name, Key=key, ConsistentRead=True)["Item"] == before
