# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""TiKV historical recovery through the signed DynamoDB API."""
import time
import uuid

import pytest
from botocore.exceptions import ClientError
from conftest import wait_for_active, wait_for_deleted
from test_auth_permissions import auth_env, mgmt, account_id, region, _make_client


def test_historical_restore(dynamodb_client, unique_table_name):
    c = dynamodb_client
    source = unique_table_name
    target = source + "-past"
    latest = source + "-now"
    try:
        c.create_table(TableName=source, KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                       AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
        wait_for_active(c, source)
        c.update_continuous_backups(TableName=source, PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True})
        c.put_item(TableName=source, Item={"pk": {"S": "key"}, "value": {"S": "before"}})
        before = c.describe_continuous_backups(TableName=source)["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]["LatestRestorableDateTime"]
        time.sleep(0.2)
        c.put_item(TableName=source, Item={"pk": {"S": "key"}, "value": {"S": "after"}})
        c.restore_table_to_point_in_time(SourceTableName=source, TargetTableName=target, RestoreDateTime=before)
        wait_for_active(c, target)
        assert c.get_item(TableName=target, Key={"pk": {"S": "key"}}, ConsistentRead=True)["Item"]["value"] == {"S": "before"}
        c.restore_table_to_point_in_time(SourceTableName=source, TargetTableName=latest, UseLatestRestorableTime=True)
        wait_for_active(c, latest)
        assert c.get_item(TableName=latest, Key={"pk": {"S": "key"}}, ConsistentRead=True)["Item"]["value"] == {"S": "after"}
        c.update_continuous_backups(TableName=source, PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": False})
        with pytest.raises(ClientError) as error:
            c.restore_table_to_point_in_time(SourceTableName=source, TargetTableName=source + "-disabled", UseLatestRestorableTime=True)
        assert error.value.response["Error"]["Code"] == "ValidationException"
    finally:
        for name in (target, latest, source):
            try:
                c.delete_table(TableName=name)
            except c.exceptions.ResourceNotFoundException:
                pass
            else:
                wait_for_deleted(c, name)


@pytest.mark.parametrize("denied_table,denied_action", [("source", "RestoreTableToPointInTime"), ("target", "PutItem")])
def test_restore_resource_deny(auth_env, mgmt, account_id, region, denied_table, denied_action):
    user = "pitr-" + uuid.uuid4().hex[:10]
    try:
        assert mgmt.create_user(account_id, user, "Pass123!").status_code == 201
        key = mgmt.create_access_key(account_id, user).json()
        policy = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
            {"Effect": "Deny", "Action": "dynamodb:" + denied_action,
             "Resource": f"arn:aws:dynamodb:{region}:{account_id}:table/{denied_table}"},
        ]}
        result = mgmt.put_user_policy(account_id, user, "deny", policy)
        assert result.status_code in (200, 201, 204), result.text
        c = _make_client(auth_env[0], key["access_key_id"], key["secret_access_key"], region)
        # Authorization precedes source lookup; neither test table needs to exist.
        with pytest.raises(ClientError) as error:
            c.restore_table_to_point_in_time(SourceTableName="source", TargetTableName="target", UseLatestRestorableTime=True)
        assert error.value.response["Error"]["Code"] == "AccessDeniedException"
    finally:
        mgmt.delete_user(account_id, user)
