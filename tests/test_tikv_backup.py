# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Large backup roundtrip through the normal signed DynamoDB API."""
import time

from conftest import wait_for_active, wait_for_deleted


def test_backup_over_four_mib(dynamodb_client, unique_table_name):
    client = dynamodb_client
    source = unique_table_name
    target = source + "-restore"
    arn = None
    try:
        client.create_table(
            TableName=source,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(client, source)
        payload = "x" * (380 * 1024)
        for i in range(12):
            client.put_item(TableName=source, Item={"pk": {"S": str(i)}, "payload": {"S": payload}})
        arn = client.create_backup(TableName=source, BackupName=source + "-backup")["BackupDetails"]["BackupArn"]
        for _ in range(120):
            details = client.describe_backup(BackupArn=arn)["BackupDescription"]["BackupDetails"]
            if details["BackupStatus"] == "AVAILABLE":
                break
            time.sleep(0.5)
        assert details["BackupStatus"] == "AVAILABLE"
        assert details["BackupSizeBytes"] > 4 * 1024 * 1024
        client.delete_item(TableName=source, Key={"pk": {"S": "11"}})
        client.restore_table_from_backup(TargetTableName=target, BackupArn=arn)
        wait_for_active(client, target)
        for i in range(12):
            item = client.get_item(TableName=target, Key={"pk": {"S": str(i)}}, ConsistentRead=True)["Item"]
            assert item["payload"] == {"S": payload}
    finally:
        for name in (target, source):
            try:
                client.delete_table(TableName=name)
            except client.exceptions.ResourceNotFoundException:
                pass
            else:
                wait_for_deleted(client, name)
        if arn:
            client.delete_backup(BackupArn=arn)
