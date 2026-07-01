from aws_cdk import Stack, RemovalPolicy
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3_deploy

from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam

from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import Duration

from constructs import Construct

class SupplyChainPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # S3 bucket creation

        bronze = s3.Bucket(self, "Bronze",
                            encryption=s3.BucketEncryption.S3_MANAGED,
                            removal_policy=RemovalPolicy.RETAIN,
                            versioned=True,
                            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
                            )
        
        silver = s3.Bucket(self, "Silver",
                           encryption=s3.BucketEncryption.S3_MANAGED,
                           removal_policy=RemovalPolicy.RETAIN,
                           versioned=True,
                           block_public_access=s3.BlockPublicAccess.BLOCK_ALL
                           )
        
        model = s3.Bucket(self, "Model",
                          encryption=s3.BucketEncryption.S3_MANAGED,
                          removal_policy=RemovalPolicy.RETAIN,
                          versioned=True,
                          block_public_access=s3.BlockPublicAccess.BLOCK_ALL
                          )
        
        scripts = s3.Bucket(self, "Scripts",
                          encryption=s3.BucketEncryption.S3_MANAGED,
                          removal_policy=RemovalPolicy.RETAIN,
                          versioned=True,
                          block_public_access=s3.BlockPublicAccess.BLOCK_ALL
                          )
        
        s3_deploy.BucketDeployment(self, "DeployGlueScript",
            sources=[s3_deploy.Source.asset("glue/")],
            destination_bucket=scripts,
            destination_key_prefix="glue"
        )
        
        # Glue Role and Job creation
        
        glue_role = iam.Role(self, "GlueJobRole",
                            assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
                            )
        
        glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
        )

        bronze.grant_read(glue_role)
        silver.grant_read(glue_role)
        scripts.grant_read(glue_role)

        etl_job = glue.CfnJob(self, "BronzeToSilverJob",
                              name="bronze-to-silver-etl",
                              role=glue_role.role_arn,
                              command=glue.CfnJob.JobCommandProperty(
                                  name="pythonshell",
                                  python_version="3.9",
                                  script_location=f"s3://{scripts.bucket_name}/glue/etl_job.py"
                              ),
                              default_arguments={
                                    "--BRONZE_BUCKET": bronze.bucket_name,
                                    "--SILVER_BUCKET": silver.bucket_name
                              },
                              max_retries=1,
                              timeout=10,
                              max_capacity=1
                              )
        
        # Lambda Trigger

        trigger_lambda = _lambda.Function(self, "TriggerLambda",
                                          runtime=_lambda.Runtime.PYTHON_3_9,
                                          handler="handler.lambda_handler",
                                          code=_lambda.Code.from_asset("src/lambda_trigger/"),
                                          timeout=Duration.seconds(30)
                                          )
        
        trigger_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:StartJobRun"],
                resources=[
                    self.format_arn(
                    service="glue",
                    resource="job",
                    resource_name=etl_job.name
                )]
            )
        )

        bronze.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(trigger_lambda)
        )
            