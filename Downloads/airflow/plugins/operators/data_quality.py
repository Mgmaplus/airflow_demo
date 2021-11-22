from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for tab in self.tables:
            self.log.info(f"Running check on {tab}")
            for check in self.dq_checks:
                sql = check.get('check_sql')
                exp_result = check.get('expected_result')

                records = redshift.get_records(sql.format(tab))
                # compare with the expected results
                records = redshift.get_records(f"SELECT COUNT(*) FROM {tab}")
                if len(records) < exp_result or len(records[0]) < exp_result:
                    raise ValueError(f"Data quality check failed. {tab} returned no results")
                num_records = records[0][0]
                if num_records < exp_result:
                    raise ValueError(f"Data quality check failed. {tab} contained 0 rows")
            self.log.info(f"Data quality on table {tab} check passed with {records[0][0]} records")
            
        self.log.info('DataQualityOperator execution completed with success for all tables')
