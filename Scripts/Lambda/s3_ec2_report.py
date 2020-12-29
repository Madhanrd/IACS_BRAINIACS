import json

from Collect_S3_Details import CommonS3

from collectCloudWatchMetrics import MetricParser

def lambda_handler(event, context):
    
    print("Welcome to Lambda!!!")

    CommonS3.main()
    
    MetricParser.main()
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
