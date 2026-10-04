# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""A peer disconnect during a large response must not harm later requests.

Close a real signed HTTP connection after its first response bytes. This works
with both Content-Length and chunked responses; it does not depend on which
urllib3 method a particular SDK happens to call.
"""
import json
import socket
import ssl
from urllib.parse import urlsplit

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest


def test_disconnect_during_large_query(dynamodb_client, create_and_cleanup_table):
    client = dynamodb_client
    name = create_and_cleanup_table(
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                              {"AttributeName": "sk", "AttributeType": "N"}],
    )["TableDescription"]["TableName"]
    payload = "x" * 60_000
    for i in range(20):
        client.put_item(TableName=name, Item={
            "pk": {"S": "partition"}, "sk": {"N": str(i)}, "payload": {"S": payload},
        })
    body = json.dumps(dict(TableName=name, KeyConditionExpression="pk = :pk",
                           ExpressionAttributeValues={":pk": {"S": "partition"}}, ConsistentRead=True))
    url = client.meta.endpoint_url
    target = urlsplit(url)
    credentials = client._request_signer._credentials.get_frozen_credentials()
    context = ssl.create_default_context()
    # Same local self-signed certificate policy as the integration-test client.
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    for _ in range(3):
        request = AWSRequest(method="POST", url=url, data=body, headers={
            "Content-Type": "application/x-amz-json-1.0", "X-Amz-Target": "DynamoDB_20120810.Query",
            "Host": target.netloc, "Content-Length": str(len(body.encode())),
        })
        SigV4Auth(credentials, "dynamodb", client.meta.region_name).add_auth(request)
        wire = ("POST / HTTP/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in request.headers.items())
                + "\r\n" + body).encode()
        with socket.create_connection((target.hostname, target.port or (443 if target.scheme == "https" else 80)), timeout=10) as raw:
            connection = context.wrap_socket(raw, server_hostname=target.hostname) if target.scheme == "https" else raw
            with connection:
                connection.sendall(wire)
                # Read only headers and then drop the response before consuming
                # its megabyte-scale body. One-byte reads avoid assuming header
                # framing matches the first TLS record.
                headers = bytearray()
                while not headers.endswith(b"\r\n\r\n"):
                    byte = connection.recv(1)
                    assert byte, "connection ended before response headers"
                    headers.extend(byte)
                    assert len(headers) < 16384
                assert headers.startswith(b"HTTP/1.1 200"), headers.decode()

    found = []
    args = json.loads(body)
    while True:
        page = client.query(**args)
        found.extend(page["Items"])
        if not page.get("LastEvaluatedKey"):
            break
        args["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    assert len(found) == 20
    assert all(item["payload"]["S"] == payload for item in found)
    client.update_item(TableName=name, Key={"pk": {"S": "partition"}, "sk": {"N": "0"}},
                       UpdateExpression="SET marker = :v", ExpressionAttributeValues={":v": {"S": "alive"}})
    assert client.get_item(TableName=name, Key={"pk": {"S": "partition"}, "sk": {"N": "0"}},
                           ConsistentRead=True)["Item"]["marker"] == {"S": "alive"}
