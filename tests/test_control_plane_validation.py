# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Control-plane rejection must preserve metadata and report client errors.

The same tests run on every backend. Pure Rust tests separately cover Unicode,
sequence arithmetic, and numeric boundaries without a running service.
"""
import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError


@pytest.fixture
def table(create_and_cleanup_table):
    return create_and_cleanup_table()["TableDescription"]


@pytest.fixture
def streams(endpoint_url):
    return boto3.client("dynamodbstreams", endpoint_url=endpoint_url,
                        region_name="us-east-1", verify=False,
                        config=Config(parameter_validation=False))


@pytest.mark.parametrize("tags", [
    [{"Key": "", "Value": "value"}],
    [{"Key": "k" * 129, "Value": ""}],
    [{"Key": "key", "Value": "字" * 257}],
    [{"Key": "key!", "Value": "value"}],
    [{"Key": "key", "Value": "value!"}],
    [{"Key": "missing"}],
])
def test_invalid_tags_preserve_existing_tags(dynamodb_client_no_validation, table, tags):
    client = dynamodb_client_no_validation
    arn = table["TableArn"]
    original = [{"Key": "kept", "Value": "before"}]
    client.tag_resource(ResourceArn=arn, Tags=original)
    with pytest.raises(ClientError, match="ValidationException"):
        client.tag_resource(ResourceArn=arn, Tags=tags)
    assert client.list_tags_of_resource(ResourceArn=arn)["Tags"] == original


def test_tag_capacity_rejects_entire_merge(dynamodb_client, table):
    client = dynamodb_client
    arn = table["TableArn"]
    original = [{"Key": f"k{i}", "Value": "before"} for i in range(50)]
    client.tag_resource(ResourceArn=arn, Tags=original)
    with pytest.raises(ClientError, match="ValidationException"):
        client.tag_resource(ResourceArn=arn, Tags=[
            {"Key": "k0", "Value": "after"}, {"Key": "extra", "Value": "bad"},
        ])
    assert sorted(client.list_tags_of_resource(ResourceArn=arn)["Tags"], key=lambda t: t["Key"]) == sorted(original, key=lambda t: t["Key"])
    client.tag_resource(ResourceArn=arn, Tags=[{"Key": "k0", "Value": "updated"}])
    assert len(client.list_tags_of_resource(ResourceArn=arn)["Tags"]) == 50


@pytest.mark.parametrize("operation", ["list_tags_of_resource", "tag_resource", "untag_resource"])
def test_subresource_tags_rejected(dynamodb_client, table, operation):
    params = {"ResourceArn": table["TableArn"] + "/index/some_index"}
    if operation == "tag_resource":
        params["Tags"] = [{"Key": "key", "Value": "value"}]
    if operation == "untag_resource":
        params["TagKeys"] = ["key"]
    with pytest.raises(ClientError, match="ValidationException.*ResourceArn"):
        getattr(dynamodb_client, operation)(**params)


@pytest.mark.parametrize("limit", [-1, 0, 101])
def test_list_streams_limit_validation(streams, limit):
    with pytest.raises(ClientError, match="ValidationException"):
        streams.list_streams(Limit=limit)


@pytest.mark.parametrize("operation", ["describe_stream", "list_streams", "get_shard_iterator"])
def test_malformed_stream_arn(streams, operation):
    arn = "malformed" * 6
    params = {"StreamArn": arn}
    if operation == "list_streams":
        params = {"ExclusiveStartStreamArn": arn}
    if operation == "get_shard_iterator":
        params.update(ShardId="shardId-" + "1" * 30, ShardIteratorType="LATEST")
    with pytest.raises(ClientError, match="ValidationException"):
        getattr(streams, operation)(**params)


def test_disable_ttl_wrong_attribute_preserves_configuration(dynamodb_client, table):
    client = dynamodb_client
    name = table["TableName"]
    client.update_time_to_live(TableName=name, TimeToLiveSpecification={"Enabled": True, "AttributeName": "expiry"})
    with pytest.raises(ClientError, match="ValidationException.*different"):
        client.update_time_to_live(TableName=name, TimeToLiveSpecification={"Enabled": False, "AttributeName": "other"})
    ttl = client.describe_time_to_live(TableName=name)["TimeToLiveDescription"]
    assert ttl["AttributeName"] == "expiry"
    assert ttl["TimeToLiveStatus"] in ("ENABLED", "ENABLING")


@pytest.mark.parametrize("spec", [{"Enabled": True}, {"AttributeName": "expiry"}])
def test_ttl_required_members_are_client_errors(dynamodb_client_no_validation, table, spec):
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb_client_no_validation.update_time_to_live(TableName=table["TableName"], TimeToLiveSpecification=spec)
    assert dynamodb_client_no_validation.describe_time_to_live(TableName=table["TableName"])["TimeToLiveDescription"]["TimeToLiveStatus"] == "DISABLED"


def test_describe_endpoints_returns_the_connected_authority(dynamodb_client):
    from urllib.parse import urlsplit
    endpoint = dynamodb_client.describe_endpoints()["Endpoints"][0]
    assert endpoint["Address"] == urlsplit(dynamodb_client.meta.endpoint_url).netloc
    assert endpoint["CachePeriodInMinutes"] > 0
