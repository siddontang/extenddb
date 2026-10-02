# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Vector ranking through the signed wire API, shared by capable backends."""
import time
import pytest
from conftest import wait_for_active, wait_for_deleted
from test_vector_index_query_scan import _signed_post

@pytest.mark.parametrize('metric,expected', [('EUCLIDEAN',['near','far']), ('COSINE',['far','near']), ('DOT_PRODUCT',['far','near'])])
def test_vector_ranking_and_mutation(dynamodb_client, unique_table_name, metric, expected):
    name=unique_table_name
    response=_signed_post('CreateTable', {
        'TableName':name,'BillingMode':'PAY_PER_REQUEST',
        'KeySchema':[{'AttributeName':'pk','KeyType':'HASH'}],
        'AttributeDefinitions':[{'AttributeName':'pk','AttributeType':'S'}],
        'VectorIndexes':[{'IndexName':'vectors','Dimensions':2,'DistanceFunction':metric,
            'VectorAttribute':{'AttributeName':'embedding'},'Projection':{'ProjectionType':'ALL'}}]})
    assert response.status_code==200, response.text
    try:
        wait_for_active(dynamodb_client,name)
        for key,vector in [('near',[1,1]),('far',[3,0])]:
            dynamodb_client.put_item(TableName=name,Item={'pk':{'S':key},'embedding':{'L':[{'N':str(v)} for v in vector]}})
        body={'TableName':name,'IndexName':'vectors','SearchVector':[{'N':'1'},{'N':'0'}],'TopK':2}
        def hits():
            result=_signed_post('SearchVectors',body)
            assert result.status_code==200,result.text
            return [h['Item']['pk']['S'] for h in result.json()['SearchResults']]
        deadline=time.monotonic()+10
        while hits()!=expected and time.monotonic()<deadline:
            time.sleep(.05)
        assert hits()==expected
        dynamodb_client.delete_item(TableName=name,Key={'pk':{'S':'far'}})
        deadline=time.monotonic()+10
        while hits()!=['near'] and time.monotonic()<deadline:
            time.sleep(.05)
        assert hits()==['near']
    finally:
        dynamodb_client.delete_table(TableName=name)
        wait_for_deleted(dynamodb_client,name)
