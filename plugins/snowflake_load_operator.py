from datetime import datetime, timedelta

from airflow.models import BaseOperator

from include.snowflake_utils import get_snowflake_conn, log_results_copy


class LoadSnowflakeFromS3Operator(BaseOperator):
    # this is required to properly render their jinja values,
    # not the jinja string itself
    template_fields = ("ingestion_start_date", "ingestion_end_date")

    def __init__(
        self,
        snowflake_conn_id: str,
        stage: str,
        schema: str,
        table: str,
        s3_prefix: str,
        file_format: str,
        truncate_table: bool = False,
        ingestion_start_date: str | None = None,
        ingestion_end_date: str | None = None,
        date_format: str = "%Y-%m-%d",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.stage = stage
        self.schema = schema
        self.table = table
        self.s3_prefix = s3_prefix
        self.file_format = file_format
        self.truncate_table = truncate_table
        self.ingestion_start_date = ingestion_start_date
        self.ingestion_end_date = ingestion_end_date
        self.date_format = date_format

    def execute(self, context) -> None:
        conn = get_snowflake_conn(conn_id=self.snowflake_conn_id)
        print(self.ingestion_end_date, self.ingestion_start_date)

        try:
            if self.truncate_table:
                truncate_sql = f"TRUNCATE TABLE {self.schema}.{self.table};"
                self.log.info(f"Executing SQL: {truncate_sql}")
                conn.execute(truncate_sql)

            print("yee")
            # loop through start_date (inclusive) and end_date (inclusive)
            # and load all files
            if self.ingestion_start_date and self.ingestion_end_date:
                print("yee2")
                start = datetime.strptime(self.ingestion_start_date, self.date_format)
                end = datetime.strptime(self.ingestion_end_date, self.date_format)
                current = start

                while current <= end:
                    print("yee3")
                    day = current.strftime("%d")

                    full_s3_prefix = f"{self.s3_prefix}/day={day}/"
                    self._load_from_s3(conn=conn, s3_prefix=full_s3_prefix)

                    current += timedelta(days=1)

            # if the start and end dates aren't provided, just load the 1 s3 prefix
            else:
                self._load_from_s3(conn, self.s3_prefix)

        finally:
            conn.close()

    def _load_from_s3(self, conn, s3_prefix: str) -> None:
        load_sql = f"""\
            COPY INTO {self.schema}.{self.table}
            FROM @{self.stage}/{s3_prefix}
            FILE_FORMAT = '{self.file_format}'
            MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
            INCLUDE_METADATA = (
                metadata_filename = METADATA$FILENAME,
                metadata_ingest_time = METADATA$START_SCAN_TIME
            );
        """
        self.log.info(f"Executing SQL: {load_sql}")

        results = conn.execute(statement=load_sql).fetchall()
        log_results_copy(results=results)
        return None
