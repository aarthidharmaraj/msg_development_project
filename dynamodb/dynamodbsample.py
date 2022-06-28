import boto3
dynamodb= boto3.resource('dynamodb')
def dynamodb_create_table():
    """This method is to create table in dynamodb"""
        
    table = dynamodb.create_table(
    TableName='config_sections',
    KeySchema=[
        {
            'AttributeName': 'section_id',
            'KeyType': 'HASH'    #Partition Key
        },
        {
            'AttributeName': 'section_name',
            'KeyType': 'RANGE'    #Sort Key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'section_id',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'section_name',
            'AttributeType': 'S'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 1,
        'WriteCapacityUnits': 1
    },
    )

def put_item_table():
    """This method is to put items on the created table in dynamodb"""
    table = dynamodb.Table('config_sections')
    table.put_item(
    Item={
        'section_id': 1,
        'section_name': 'freesound_api',
        's3_path':'freesound_api/source',
        'bucket': 'msg-practice-induction',
        'basic_url':'https://freesound.org/apiv2/',
        'fernet_key':'5EIHJePaBEaVlhLdk5aZT3hZ-vtDnAdSuHzfB_GRd7Q=',
        'api_key ': 'gAAAAABinfOH1Um-c_8wlKMfL46pVyer0vPQtM872VoFPilA1KhoEI6mzyUdpGEp4HbjKj7aZcxCMIL8PpH_PAz2m2eXVjWXBlueVHmkjj3OTlq9dE8Y9BygupzIcB5WxpmJpVUgGucU',
    },
    )
    table.put_item(
    Item={
        'section_id': 2,
        'section_name': 'eBird_api',
        's3_path':'eBird_api/source',
        'bucket': 'msg-practice-induction',
        'basic_url':' https://api.ebird.org/v2/',
        'fernet_key':' ws9quFELNDWIzhBemsHgqbqFnEyR3-av0G_FJiEEqYo=',
        'api_key ': 'gAAAAABimGHBfNMXAEmcJqYAVzqzjrV1CxZXVh0YX2tgejSb7NwP9bLqYZUrHGsTZGp_RcujDveN68Orcr1v2h3vzq7EB-uK6Q=='
    },
)
def get_item():
    """this method gets the item from the table"""
    table = dynamodb.Table('config_sections')
    resp = table.get_item(
    Key={
        'section_id': 1
    },
    )
    print(resp['Item'])

def update_item():
    """This method updates the item in the table"""
    table = dynamodb.Table('config_sections')
    resp = table.update_item(
    Key={'section_id': 1},
    UpdateExpression="SET s3_path= :s",
    ExpressionAttributeValues={':s': 'source/example'},
    ReturnValues="UPDATED_NEW"
    )
    print(resp['Attributes'])