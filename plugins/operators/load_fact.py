from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Insert data from staging tables into the fact table.
    The target table is checked first if we want to append or not.
    
    Keyword arguments:
    redshift_conn_id   -- the connection id for redshift.
    sql_query          -- the SQL insert statement.
    table_name         -- the name of the table to inset data into.
    append_only        -- boolean variable to choose either append or not.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query = "",
                 table_name = "",
                 append_only = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if not self.append_only:
            self.log.info("Delete Befor insert the  fact table {}".format(self.table_name))
            redshift_hook.run("DELETE FROM {}".format(self.table_name))         
        self.log.info("Insert data from staging tables into the fact table {}".format(self.table_name))
        
        redshift_hook.run(self.sql_query)