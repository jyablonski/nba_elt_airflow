from sqlalchemy import exc, create_engine
import logging
import os

def aws_connection(schema: str):
    try:
        connection = create_engine(f'postgresql+psycopg2://' + os.environ.get('RDS_USER') + ':' + os.environ.get('RDS_PW') + '@' + os.environ.get('IP') + ':' + '5432' + '/' + os.environ.get('RDS_DB'),
                                    connect_args = {'options': '-csearch_path=' + schema}, # defining schema to connect to
                     echo = False)
        logging.info(f'SQL Connection to {schema} Successful')
        print(f'SQL Connection to {schema} Successful')
        return(connection)
    except exc.SQLAlchemyError as e:
        logging.info(f'SQL Connection to {schema} Failed, Error: {e}')
        print(f'SQL Connection to {schema} Failed, Error: {e}')
        return(e)