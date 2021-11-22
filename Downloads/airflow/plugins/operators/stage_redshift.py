from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"
    template_fields = ("s3_key",)
    staging_copy = """
        COPY {} 
        FROM '{}' 
        access_key_id '{}'
        secret_access_key '{}'
        json '{}' 
        region 'us-west-2';
    """        
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 aws_credentials_id="",
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials =  aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Credentials loaded and Redshift hook established')
                           
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info("Clearing data from staging")
        redshift.run(f"DELETE FROM {self.table}")
        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.staging_copy.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        redshift.run(formatted_sql)
        
                    





