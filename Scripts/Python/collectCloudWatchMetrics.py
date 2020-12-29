import boto3
import boto3.session
import botocore
from  datetime import datetime, timedelta
import pandas as pd
import numpy as np
from io import StringIO
from tabulate import tabulate

  


class MetricParser:
    def __init__(self, df):
        self.cpu_util_df = df

    def getIamDetails(self, resource, instance_id):
        try:
            filter = {
                'Name': 'instance-id',
                'Values': [instance_id]
            }
            iam_details = []
            response = resource.describe_iam_instance_profile_associations(
                Filters = [filter]
            )
            for record in response['IamInstanceProfileAssociations']:
                iam_details.append(record['IamInstanceProfile']['Arn'])
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'INSTANCE_IAM' ] = iam_details if iam_details else 'NA'
        except botocore.exceptions.ClientError as e:
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID'] == instance.id, "INSTANCE_IAM"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getTagDetails()")

    def getTagDetails(self, resource, instance_id):
        try:         
            tag_details = []
            filter = {
                'Name': 'resource-id', 
                        'Values': [instance_id]
            }
            response = resource.describe_tags(
                    Filters = [ filter ]
                )
          
            for i in response['Tags']:
                tag  = {}
                if i['Key'] in 'Name':
                    instance_name = i['Value']
                    print(instance_name)
                tag['key'] = i['Key']
                tag['value'] = i['Value']
                tag_details.append(tag)
            tag_details = ",".join([item['key']+":"+item['value'] for item in tag_details])
            # Adding the Tag Details to the DataFrame 
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'INSTANCE_TAGS' ] = tag_details #pd.Series([tag_details])
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'INSTANCE_NAME' ] = instance_name
        except botocore.exceptions.ClientError as e:
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID'] == instance.id, "INSTANCE_NAME"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getTagDetails()")

    def getSecurityGroupDetails(self,instance, instance_id):
        security_gp_name = []
        for i in instance.security_groups:
            security_gp_name.append(i['GroupName'])
        try:
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'INSTANCE_SG'] = security_gp_name
        except botocore.exceptions.ClientError as e:
            print(e.response['Error']['Message'])
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID'] == instance_id, "INSTANCE_SG"] = 'ERROR'
        except Exception as e1:
            print(e1)
            print("Oops!", e1.__class__, "occurred in the getSecurityGroup()")

    def getInstanceStatus(self, instance, instance_id ):
        try:
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  ==  instance_id, 'INSTANCE_STATUS'] = instance.state['Name']
        except botocore.exceptions.ClientError as e:
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID'] == instance_id, "INSTANCE_STATUS"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getInstanceStatus()")

    def getInstanceDetails(self, instance, instance_id ):
        # Parse Information and add it to Dataframe
        try:            
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  ==  instance_id, 'INSTANCE_TYPE'] = instance.instance_type
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'INSTANCE_LAUNCH_TIME'] = instance.launch_time
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'INSTANCE_PRV_IP'] = instance.private_ip_address
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'INSTANCE_PUB_IP'] = instance.public_ip_address if instance.public_ip_address  else 'NONE'
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'INSTANCE_SUBNET_ID'] = instance.subnet_id
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'INSTANCE_VPC_ID'] = instance.vpc_id

        except botocore.exceptions.ClientError as e:
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID'] == instance_id, "INSTANCE_NAME"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getInstanceDetails()")

    def writeData(self, df, bucket, objectname,  s3_resource):
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)        
            s3_resource.Object(bucket, objectname).put(Body=csv_buffer.getvalue())
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the writeData()")


    def buildQuery(self, instance_id, period, start_time, end_time, metricVar, stat='Maximum'):
        response = client.get_metric_statistics(
                    Namespace = 'AWS/EC2',
                    Period = period,
                    StartTime = start_time,  
                    EndTime = end_time, 
                    MetricName = metricVar,
                    Statistics=[stat],
                    Dimensions = [
                    {'Name': 'InstanceId', 'Value': instance_id}
                ])

        return response

    def logicBuilder(self,cpu_util_df,instance_id):
        try:            
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'CPU_UTIL_STATUS' ] =  cpu_util_df['CPU1'].apply(lambda x: 'OVER_UTILIZED' if x > 80 else 'AVAILABLE')
        except botocore.exceptions.ClientError as e:
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID'] == instance_id, "CPU_UTIL_STATUS"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getInstanceDetails()")

    def StatusCheck(self,cpu_util_df,instance_id):
        try:            
            # Calculating Average Value 
            #cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'one_hour_utils' ]  = ( cpu_util_df['P1'] + cpu_util_df['P2'] + cpu_util_df['P3'] + cpu_util_df['P4'] + cpu_util_df['P5'] + cpu_util_df['P6'] + cpu_util_df['P7'] + cpu_util_df['P8'] + cpu_util_df['P9'] + cpu_util_df['P10'] + cpu_util_df['P11'] ) / 11
            #cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'one_hour_utils' ]  = ( cpu_util_df['P1'] + cpu_util_df['P2'] + cpu_util_df['P3'] + cpu_util_df['P4']  ) / 11
            # Get tPe Cpu Status Based on the onehour_utils Value 
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'STATUSCHECK' ] =  cpu_util_df['STATUS_VALUE'].apply(lambda x: 'PASSED' if x == 0 else 'FAILED')
        except botocore.exceptions.ClientError as e:
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID'] == instance_id, "STATUSCHECK"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getInstanceDetails()")

    def Metricutilization(self, df, response, instance_id,column_name):
        network_value = {}
        for i in response['Datapoints']:
            network_value[i['Timestamp']] = i['Maximum']
        #print(f"Length of DataPoints in the Metric {len(network_value.keys())}")
        hour = 1
        for key in sorted(network_value.keys()):
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, column_name ] = network_value[key]
            hour = hour - 1



    def cpuUtilization(self, cpu_util_df, response, instance_id):
        metricvalue = {}
        for i in response['Datapoints']:
            metricvalue[i['Timestamp']] = i['Maximum']        
        
        print(f"Length of DataPoints in the Metric {len(metricvalue.keys())}")
        # for key in sorted(metricvalue.keys()):
        #     print("%s: %s" % (key, metricvalue[key]))
        hour = 1
        for key in sorted(metricvalue.keys()):
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'CPU'+str(hour) ] = metricvalue[key]
            hour = hour - 1


if __name__ == "__main__":
    try:
        # Defining Bucket Details 
        output_bucket='brain-iacs-east'
        objectname='ec2-metrics/Ec2cloudwatchmetric.csv'
        # Create S3 Resource to write output data  
        s3_resource = boto3.resource('s3')

        # Define the DataFrame Columns 
        columns= ['INSTANCE_ID',  'INSTANCE_NAME', 'INSTANCE_TYPE',  'REGION', 'INSTANCE_STATUS', 'INSTANCE_LAUNCH_TIME', 'INSTANCE_PRV_IP', 'INSTANCE_PUB_IP', 'INSTANCE_SUBNET_ID', 'INSTANCE_VPC_ID', 'INSTANCE_SG', 'INSTANCE_IAM','INSTANCE_TAGS', 'CPU_UTIL_STATUS',  'STATUSCHECK', 'CPU1', 'NETIN', 'NETOUT', 'DISKREAD', 'DISKWRITE', 'STATUS_VALUE']
        cpu_util_df = pd.DataFrame(columns= columns)

        cw_collector = MetricParser(cpu_util_df)
        collector = MetricParser(cpu_util_df)
        # Defining the Regions 
        regions = ['us-east-1', 'us-west-1']
        for region in regions:
            print(f"For Region {region}")
            my_session = boto3.session.Session(region_name=region)
            
            # Create Ec2 Resources 
            ec2_resource = my_session.resource('ec2')
            ec2_client = my_session.client('ec2')

            # Ccreate Cloudwatch Object 
            client = my_session.client(service_name='cloudwatch', region_name=region)
            for instance in ec2_resource.instances.all():
                instance_id = instance.id
                print(instance_id)
                cpu_util_df = cpu_util_df.append({'INSTANCE_ID': instance_id}, ignore_index=True)
                cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'REGION' ] = region
                # Collecting Ec2 Meta Data
                collector.getTagDetails(ec2_client, instance_id)
                collector.getInstanceDetails(instance, instance_id)
                collector.getInstanceStatus(instance, instance_id)
                collector.getSecurityGroupDetails(instance, instance_id)
                collector.getIamDetails(ec2_client, instance_id)

                # Metrics Start And End Time 
                start_time = datetime.utcnow() -  timedelta(minutes = 15)
                end_time = datetime.utcnow()
                # Setting the DataPoints Period 0f 15 Minutes 15*60 = 900 
                period = 900
                response = cw_collector.buildQuery( instance_id, period,start_time, end_time,'CPUUtilization')
                #print(response)
                #
                # Collecting Response from the Cloud Watch 
                network_in_response = cw_collector.buildQuery( instance_id, period,start_time, end_time,'NetworkIn', )
                network_out_response = cw_collector.buildQuery( instance_id, period,start_time, end_time,'NetworkOut', )
                diskRead_response = cw_collector.buildQuery( instance_id, period,start_time, end_time,'DiskReadBytes')
                diskWrite_response = cw_collector.buildQuery( instance_id, period,start_time, end_time,'DiskWriteBytes')
                status_response = cw_collector.buildQuery( instance_id, period,start_time, end_time,'StatusCheckFailed')
                #print(status_response)
                # Parsing The Response and Add it To DataFrame 
                cw_collector.cpuUtilization(cpu_util_df,response,instance_id)
                cw_collector.Metricutilization(cpu_util_df,network_in_response,instance_id,'NETIN')
                cw_collector.Metricutilization(cpu_util_df,network_in_response,instance_id,'NETOUT')
                cw_collector.Metricutilization(cpu_util_df,diskRead_response,instance_id,'DISKREAD')
                cw_collector.Metricutilization(cpu_util_df,diskWrite_response,instance_id,'DISKWRITE')
                cw_collector.Metricutilization(cpu_util_df,diskWrite_response,instance_id,'STATUS_VALUE')
                            
                 
                # Calculating the Status
                cw_collector.logicBuilder(cpu_util_df,instance_id)
                cw_collector.StatusCheck(cpu_util_df,instance_id)
        #       
        #print(tabulate(cpu_util_df, headers='keys', tablefmt='psql'))
        
        # East Data Frame 
        my_east_df = cpu_util_df[cpu_util_df['REGION']  == 'us-east-1']
        # West Data Frame 
        my_west_df = cpu_util_df[cpu_util_df['REGION']  == 'us-west-1']
        
        # Joining Two DataFrames
        result_df  = pd.merge(my_east_df,my_west_df,on='INSTANCE_NAME', how='outer')
        
        # Adding Conditions for Reporting 
        result_df['InstanceTypeFlag'] = np.where((result_df['INSTANCE_TYPE_x']==result_df['INSTANCE_TYPE_y']),'Matched','Not Matched')
        result_df['InstanceSGFlag'] = np.where((result_df['INSTANCE_SG_x']==result_df['INSTANCE_SG_y']),'Matched','Not Matched')
        result_df['InstanceIAMFlag'] = np.where((result_df['INSTANCE_IAM_x']==result_df['INSTANCE_IAM_y']),'Matched','Not Matched')
        # Print the Column Details 
        #print(result_df.info())
        choice_color_list=('G','R')
        conditions_color=(result_df.iloc[:,41:43].eq('Matched').all(1),result_df.iloc[:,41:43].eq('Not Matched').all(1))
        result_df['ColorFlag']=np.select(conditions_color,choice_color_list,default='O')

        print(tabulate(result_df, headers='keys', tablefmt='psql'))
        #print(tabulate(my_west_df, headers='keys', tablefmt='psql'))

        # Writing Data to S3 Bukcet 
        cw_collector.writeData(result_df,output_bucket,objectname, s3_resource)
    except Exception as e:
        print("Oops!", e, ". Check the __main__() Function")
                
   
