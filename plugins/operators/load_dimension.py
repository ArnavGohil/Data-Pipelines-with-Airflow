from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table_name="",
                 sql_statement="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_statement = sql_statement
        self.append_data = append_data

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Adding data to {self.table_name} dimention table.')
        if self.append_data != True:
            redshift_hook.run(f"DELETE FROM {self.table_name}")
        redshift_hook.run(f"""INSERT INTO {self.table_name} 
                              {self.sql_statement} ;""")
