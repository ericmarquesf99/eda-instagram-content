import pandas as pd
import logging
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connection to PostgreSQL parameters
db_params = {
    'host': 'tuffi.db.elephantsql.com',
    'database': 'ezjoiktc',
    'user': 'ezjoiktc',
    'password': 'V08E4u_lljnKTRq3dMTmH0sGReUP_6NM'
}

# Variáveis de conexão
user = 'ezjoiktc'
password = 'V08E4u_lljnKTRq3dMTmH0sGReUP_6NM'
host = 'tuffi.db.elephantsql.com'
database = 'ezjoiktc'

connection_string = f'postgresql://{user}:{password}@{host}/{database}'

# PostgreSQL table name
schema_name = 'gerandoafetospsi'
nome_tabela = 'teste'


# Connecting to PostgreSQL using psycopg2
def load_post_metadata(df):
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Load Dataframe data into PostgreSQL table
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]
        execute_values(cursor, f"INSERT INTO {schema_name}.{nome_tabela} ({', '.join(df.columns)}) VALUES %s", values, template=None, page_size=100)

        connection.commit()

        print(f"CSV file data loaded successfully into the table '{nome_tabela}'.")

    except Exception as e:
        print(f"Connection or loading error: {e}")

    finally:
        # Close connection when finished
        if connection:
            cursor.close()
            connection.close()
            print("Connection closed.")

def load_post_hashtags(df):
    try:
        table_name = 'gerandoafetospsi.tb_posts_hashtags'
        engine = create_engine(connection_string)

        # Inserir dados na tabela no PostgreSQL
        print(f'schema_name: {schema_name}') 
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        # Adicionar mensagem de log ao finalizar a inserção
        logger.info(f'Dados inseridos na tabela {table_name} com sucesso.')
    except SQLAlchemyError as e:
        # Log da exceção e informações de troubleshooting
        logger.error(f'Erro durante a inserção de dados na tabela {table_name}: {str(e)}')

