import boto3,os
from botocore.exceptions import ClientError
from influxdb import InfluxDBClient
from dotenv import load_dotenv
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")
def get_movie(title, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://dynamodb.us-west-2.amazonaws.com",
                                  region_name="us-west-2", aws_access_key_id=AWS_ACCESS_KEY_ID,
                                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    table = dynamodb.Table('Battery_Pack_Data')

    try:
        response = table.get_item(Key={'deviceid': title, 'timestamp': "1532199319045"})
    except ClientError as e:
        print(e)
        print(e.response['Error']['Message'])
    else:
        return [response['Item']]


def get_all(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://dynamodb.us-west-2.amazonaws.com",
                                  region_name="us-west-2", aws_access_key_id=AWS_SECRET_ACCESS_KEY,
                                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    table = dynamodb.Table('Battery_Pack_Data')

    response = table.scan()
    data = response['Items']

    all_data = []
    num = 0
    while 'LastEvaluatedKey' in response:
        num = int(num) + 1
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
        for i in response['Items']:
            all_data.append(i)
        print(num, len(all_data))

    return all_data


def write_to_influxdb(all_data):
    try:
        client = InfluxDBClient(host='3.22.63.228', port='8086', username='ashen', password='1234', database='DynamoDB')
    except Exception as e:
        client = None
        print(e)

    client.drop_database('DynamoDB')
    client.create_database('DynamoDB')

    metric = 'Battery_Pack_Data'
    datapoints = []
    batchsize = 10000
    count = 0

    for data in all_data:
        timestamp = int(int(data["timestamp"])/1000)
        deviceid = data["deviceid"]
        item_data = data["data"]
        Avg_Current = float(item_data['Avg_Current'])
        Temp_Top = float(item_data["Temp_Top"])
        Remaining_Capacity = float(item_data["Remaining_Capacity"])
        State_of_Health = float(item_data["State_of_Health"])
        Absolute_SoC = float(item_data["Absolute_SoC"])
        Cycle_Count = float(item_data["Cycle_Count"])
        Relative_SoC = float(item_data["Relative_SoC"])
        VAux_Voltage = float(item_data["VAux_Voltage"])
        Avg_TTE = float(item_data["Avg_TTE"])
        Run_TTE = float(item_data["Run_TTE"])
        Avg_TTF = float(item_data["Avg_TTF"])
        Temp_Middle = float(item_data["Temp_Middle"])
        Ext_Avg_Cell_Voltage = float(item_data["Ext_Avg_Cell_Voltage"])
        Full_Capacity = float(item_data["Full_Capacity"])
        Temp_Bottom = float(item_data["Temp_Bottom"])
        Current = float(item_data["Current"])
        Pack_Voltage = float(item_data["Pack_Voltage"])

        fields = {
            "deviceid": str(deviceid),
            "Avg_Current": Avg_Current,
            "Temp_Top": Temp_Top,
            "Remaining_Capacity": Remaining_Capacity,
            "State_of_Health": State_of_Health,
            "Absolute_SoC": Absolute_SoC,
            "Cycle_Count": Cycle_Count,
            "Relative_SoC": Relative_SoC,
            "VAux_Voltage": VAux_Voltage,
            "Avg_TTE": Avg_TTE,
            "Run_TTE": Run_TTE,
            "Avg_TTF": Avg_TTF,
            "Temp_Middle": Temp_Middle,
            "Ext_Avg_Cell_Voltag": Ext_Avg_Cell_Voltage,
            "Full_Capacity": Full_Capacity,
            "Temp_Bottom": Temp_Bottom,
            "Current": Current,
            "Pack_Voltage": Pack_Voltage,
        }

        tags = {
            "deviceid": str(deviceid),
        }

        point = {"measurement": metric, "time": timestamp, "fields": fields, "tags": tags}

        datapoints.append(point)

        if len(datapoints) % batchsize == 0:
            print('Inserting from {} to {} datapoints...'.format(batchsize * count, (batchsize * (count + 1))))
            count = count + 1
            response = None
            try:
                response = client.write_points(datapoints, batch_size=batchsize,time_precision='s')
            except Exception as e:
                print(e)

            if not response:
                print('Problem inserting points, exiting...')
                exit(1)

            datapoints = []
            print("Wrote %d points, response: %s" % (batchsize, response))

    print('Inserting from {} to {} datapoints...'.format(batchsize * count, len(all_data)))
    response = None
    try:
        response = client.write_points(datapoints, batch_size=batchsize, time_precision='s')
    except Exception as e:
        print(e)

    if not response:
        print('Problem inserting points, exiting...')
        exit(1)
    print("Wrote %d points, response: %s" % (len(datapoints), response))


if __name__ == '__main__':
    # all_data = get_movie("5042394E3530690E121E", )
    all_data = get_all()
    write_to_influxdb(all_data=all_data)
