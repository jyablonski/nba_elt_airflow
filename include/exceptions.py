from airflow import AirflowException


class NoConnectionExists(AirflowException):
    pass


class S3PrefixCheckFail(AirflowException):
    pass


class SnowflakeCheckError(AirflowException):
    pass
