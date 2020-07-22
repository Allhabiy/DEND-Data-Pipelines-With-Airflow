from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    """
    Performs data quality checks on all tables.
    The check if each table have at least 1 row.
    
    Keyword arguments:
    redshift_conn_id   -- the connection id for redshift.
    tables             -- the list of all tables to check it.
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        
    def check_if_rows(self, redshift_hook, table):
        records = redshift_hook.get_records(f"select count(*) from {table};")    
        if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
            raise ValueError(f"Data Quality validation failed for table : {table}")
        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        for table in self.tables:
            self.log.error(f"Data Quality validation failed for table : {table}.")
            self.check_if_rows(redshift_hook, table)
            
        self.log.info(f"Data Quality Validation Passed on table : {table}!!!")  