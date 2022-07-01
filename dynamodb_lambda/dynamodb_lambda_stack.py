from aws_cdk import (
    aws_lambda,
    aws_dynamodb,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_lambda_event_sources as lambda_event_source,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_events,
    Stack,
    Duration
)
from constructs import Construct

class DynamodbLambdaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # Define a standard SQS queue. 
        queue = sqs.Queue(
             self, "pocqueue_cdk",
             visibility_timeout=Duration.seconds(600));

        # Fetch the existing SNS topic arn (This is where CY_Exceptions topic needs to be placed)
        topic = sns.Topic.from_topic_arn(
            self,
            "poctopic",
            "arn:aws:sns:us-east-2:192893522546:poctopic"
        );

        # Add a sqs subscription on an existing SNS topic

        topic.add_subscription(subscriptions.SqsSubscription(queue));

        # Create a IAM role first which should have read privileges from SQS & write to DDB: Pending
        # Privileges Needed: AWSLambdaBasicExecutionRole & AmazonDynamoDBFullAccess

        role = iam.Role(scope=self, id='testcdkrole',
                assumed_by=iam.ServicePrincipal(service='lambda.amazonaws.com'),
                role_name = 'Lambda_CDK_Role',
                managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name(
                        managed_policy_name='service-role/AWSLambdaBasicExecutionRole')
                ])

        role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=["dynamodb:DescribeTable","dynamodb:ListTables"],
                resources=["*"]
            )
        )

        

        # Deploy the Lambda Stack now within lambda folder

        main_handler = aws_lambda.Function(self, 'poc-lambda-cdk',
                                           runtime=aws_lambda.Runtime.PYTHON_3_7,
                                           function_name ='demo-sqs-poc-lambda-cdk',
                                           description = 'Lambda function deployed using CDK',
                                           code =aws_lambda.Code.from_asset('./lambda'),
                                           handler = 'lambda_function.lambda_handler'
                                           );


        # Add a lambda trigger on the SQS queue
        main_handler.add_event_source(lambda_event_source.SqsEventSource(queue,
            batch_size=10, #default
            report_batch_item_failures =True
        ));

        # Add another DDB source table with dynamodb streams enabled (This would just be for internal testing. This section won't be present on production)


        # Define the target DDB table attributes
        aws_eventbridge_scd2_cdk = dynamodb.Table(self, "aws_eventbridge_scd2_cdk",
                                                  table_name = 'aws-eventbridge-scd2-cdk',
                                                  partition_key=dynamodb.Attribute(name="ID",type=dynamodb.AttributeType.NUMBER),
                                                  sort_key=dynamodb.Attribute(name="END_DATETIMESTAMP", type=dynamodb.AttributeType.STRING)
                                                 );

        # We can utilize grant method for giving Lambda the access required to write to DDB table. 
        # This is optional or another workaround since logically the best practice is to create an IAM role & attach required ploicies to it
        aws_eventbridge_scd2_cdk.grant(main_handler);
