import logging
import psycopg2
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connection variables
user = 'ezjoiktc'
password = 'V08E4u_lljnKTRq3dMTmH0sGReUP_6NM'
host = 'tuffi.db.elephantsql.com'
database = 'ezjoiktc'

# Connecting to PostgreSQL
connection_string = f'postgresql://{user}:{password}@{host}/{database}'
conn = psycopg2.connect(connection_string)

def load_post_metadata(df):
    try:
        schema_name = 'gerandoafetospsi'
        table_name = 'tb_posts_metadata'
        aux_table_name = 'tb_posts_aux'

        # Open cursor
        cursor = conn.cursor()

        # Truncate aux table before loading
        truncate_query = f"TRUNCATE TABLE {aux_table_name};"
        cursor.execute(truncate_query)

        # Inserting data into aux table
        # Load Dataframe data into PostgreSQL table
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]
        execute_values(cursor, f"INSERT INTO {schema_name}.{aux_table_name} ({', '.join(df.columns)}) VALUES %s", values,
                       template=None, page_size=100)

        """
        # Inserting only non-existing records in post metadata main table.
        insert_main_table = f"INSERT INTO {table_name} SELECT * FROM {aux_table_name} AUX WHERE NOT EXISTS" \
                            f"(SELECT 1 FROM {table_name} MAIN WHERE AUX.post_id = MAIN.post_id);"
        cursor.execute(insert_main_table)
        """
        # Commit para aplicar as alterações
        conn.commit()

        # Fechar a conexão e o cursor
        cursor.close()
        conn.close()

        # Adicionar mensagem de log ao finalizar a inserção
        logger.info(f'Dados inseridos na tabela {table_name} com sucesso.')
    except psycopg2.Error as e:
        logger.error(f"Erro ao tentar carregar a tabela: {e}")
        logger.error("Detalhes adicionais:")
        logger.error(f"Query executada: {execute_values}")
        logger.error(f"Dados a serem inseridos: {df.to_dict(orient='records')}")


def load_post_hashtags(df):
    try:
        table_name = 'gerandoafetospsi.tb_posts_hashtags'

        # Abrir um cursor para executar comandos SQL
        cursor = conn.cursor()

        # Truncate table before loading
        truncate_query = f"TRUNCATE TABLE {table_name};"
        cursor.execute(truncate_query)

        # Iterar sobre as linhas do DataFrame e inserir os dados na tabela
        for index, row in df.iterrows():
            insert_query = f"INSERT INTO {table_name}  VALUES ('{row['post_id']}','{row['hashtags']}');"
            cursor.execute(insert_query)

        # Commit para aplicar as alterações
        conn.commit()

        # Fechar a conexão e o cursor
        cursor.close()
        conn.close()

        # Adicionar mensagem de log ao finalizar a inserção
        logger.info(f'Dados inseridos na tabela {table_name} com sucesso.')
    except psycopg2.Error as e:
        logger.error(f"Erro ao tentar carregar a tabela: {e}")

