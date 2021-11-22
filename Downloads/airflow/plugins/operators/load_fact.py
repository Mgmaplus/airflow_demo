from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 destination_table = "",
                 sql_statement = "",
                 append_only = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_statement = sql_statement
        self.append_only = append_only

    def execute(self, context):
        self.log.info(f'Loading {self.destination_table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_only == False:
            self.log.info(f"Deleting {self.destination_table}")
            redshift.run(f"DELETE FROM {self.destination_table}")
           
        redshift.run(f"INSERT INTO {self.destination_table} {self.sql_statement}")
        
        self.log.info(f'Loaded {self.destination_table}')
            