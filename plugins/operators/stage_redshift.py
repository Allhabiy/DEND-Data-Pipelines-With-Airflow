from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """"
    Copies JSON files from an S3 bucket into a Redshift staging table.
    The table is cleared first if it exists.
    
    Keyword arguments:
    redshift_conn_id   -- the connection id for redshift.
    aws_credential_id  -- credentails used to access AWS
    table_name         -- the name of the table to inset data into.
    s3_bucket          -- suffix of the S3 bucket.
    s3_key             -- prefix of the S3 bucket.
    copy_json_option   -- the path to JSON formatter.
    region             -- the region of the Redshift cluster.
    data_format        -- specify the format of your data.
    """
    
    ui_color = '#358140'
    
    copy_query = "COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' REGION '{}' {} '{}'"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id="",
                 table_name = "",
                 s3_bucket="",
                 s3_key = "",
                 copy_json_option='',
                 region="",
                 data_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.region = region
        self.data_format= data_format
        self.execution_date = kwargs.get('execution_date')
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()

        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f"Picking staging file for table {self.table_name} from location : {s3_path}")
       
        copy_query = self.copy_query.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, self.region, self.data_format, self.copy_json_option)
       
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Deleting data from the Redshift table destination")
        redshift_hook.run("DELETE FROM {}".format(self.table_name))
        
        redshift_hook.run(copy_query)
        
        self.log.info(f"Table {self.table_name} staged successfully!!")