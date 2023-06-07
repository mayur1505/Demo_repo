import json
import boto3

dynamoresource = boto3.resource('dynamodb')
table = dynamoresource.Table('Automation_script_ECS')
ecsclient = boto3.client('ecs')
dynamoclient = boto3.client('dynamodb')


### Container will spawn after a few moments
def startcontainer(clusterName):
    print("The process of starting cluster services has been started and the table is ", dynamoclient)

    data = dynamoclient.scan(
        TableName='Automation_script_ECS', ScanFilter={
            "Cluster": {
                "AttributeValueList": [{"S": clusterName}],
                "ComparisonOperator": "EQ"
            }
        }
    )
    print("The data is ", data)
    services = []
    # print(json.dumps(data, indent=4))
    for each_item in data['Items']:
        print("The service to start is ", each_item['serviceArns']['S'])
        print("The desiredCount to start is ", each_item['desiredCount']['N'])
        # if each_item['Cluster']['S'] == clusterName:
        services.append({
            "arn": each_item['serviceArns']['S'],
            "desiredCount": each_item['desiredCount']['N'],
        })

    for srv in services:
        try:
            print("The starting begins")
            responseUpdate = ecsclient.update_service(
                cluster=clusterName,
                service=srv["arn"],
                desiredCount=int(srv["desiredCount"]),
            )
        except:
            print("service not found for", srv["arn"])
            pass

    # for srv in services:
    #     print("The starting begins")
    #     responseUpdate = ecsclient.update_service(
    #         cluster=clusterName,
    #         service=srv["arn"],
    #         desiredCount=int(srv["desiredCount"]),
    #     )


### Sets the desired count of tasks per service to 0
### Services still runs but without any container
def stopcontainer(clusterName):
    new_arn_list = []
    print("The name of the cluster in stopcontainer is ", clusterName)

    # updating data dynamically for given cluster
    paginator = ecsclient.get_paginator('list_services')
    response_iterator = paginator.paginate(
        cluster=clusterName,
        PaginationConfig={
            'PageSize': 100
        }
    )

    for each_page in response_iterator:
        print('page is ', each_page)
        for each_arn in each_page['serviceArns']:
            print('each arn', each_arn)
            new_arn_list.append(each_arn)
            print("The new arn is ", new_arn_list)
            response = ecsclient.describe_services(cluster=clusterName,
                                                   services=[each_arn])
            # print(json.dumps(response, indent=4, default=str))
            # cluster_name_small = clusterName.split("cluster/")[1]  # for selecting only cluster name
            data = table.update_item(
                Key={'Cluster': clusterName,
                     'serviceArns': each_arn
                     },
                UpdateExpression="set desiredCount= :d",
                ExpressionAttributeValues={

                    ':d': response["services"][0]["desiredCount"]
                },
                ReturnValues="UPDATED_NEW"
            )

    for arn in new_arn_list:
        responseUpdate = ecsclient.update_service(
            cluster=clusterName,
            service=arn,
            desiredCount=0,
        )


def lambda_handler(event, context):
    ### The Amazon Eventbridge rule sends inputs in json format in which cluster name is mentioned.
    cluster = event["cluster"]

    # cluster = ["r2"]

    # cluster = ['rubicon-staging', 'rubicon-test', 'Integrace-staging','Integrace-test']
    # ### The Amazon Eventbridge rule sends action(start or stop) in json format to start or stop the  cluster.
    action = event["action"]
    # action = "start"

    for clusterName in cluster:

        print("action is", action)
        print("cluster is", clusterName)
        print("The type of cluster is", type(clusterName))

        if 'start' == action:
            startcontainer(clusterName)

        elif 'stop' == action:
            stopcontainer(clusterName)
