import logging
import psycopg2
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connection variables
user = ''
password = ''
host = ''
database = ''

# Connecting to PostgreSQL
connection_string = f'postgresql://{user}:{password}@{host}/{database}'
conn = psycopg2.connect(connection_string)

def load_post_metadata(df):
    try:
        schema_name = 'gerandoafetospsi'
        table_name = 'tb_posts'
        aux_table_name = 'tb_posts_aux'

        # Open cursor
        cursor = conn.cursor()

        # Truncate aux table before loading
        truncate_query = f"TRUNCATE TABLE {schema_name}.{aux_table_name};"
        cursor.execute(truncate_query)

        # Inserting data into aux table
        # Load Dataframe data into PostgreSQL table
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]
        execute_values(cursor, f"INSERT INTO {schema_name}.{aux_table_name} ({', '.join(df.columns)}) VALUES %s", values,
                       template=None, page_size=100)

        # Inserting only non-existing records in post metadata main table.
        insert_main_table = f"INSERT INTO {schema_name}.{table_name} SELECT * FROM {schema_name}.{aux_table_name} AUX WHERE NOT EXISTS" \
                            f"(SELECT 1 FROM {schema_name}.{table_name} MAIN WHERE AUX.post_id = MAIN.post_id);"
        cursor.execute(insert_main_table)

        # Commit to apply changes
        conn.commit()

        # End connection and cursor
        cursor.close()
        conn.close()

        # Troubleshooting
        logger.info(f'Data inserted on {table_name} successfully.')
    except psycopg2.Error as e:
        logger.error(f"Error while loading the table: {e}")
        logger.error("Additional details:")
        logger.error(f"Query executed: {execute_values}")
        logger.error(f"Data to be inserted: {df.to_dict(orient='records')}")


def load_post_hashtags(df):
    try:
        table_name = 'gerandoafetospsi.tb_posts_hashtags'

        # Opening cursor
        cursor = conn.cursor()

        # Truncate table before loading
        truncate_query = f"TRUNCATE TABLE {table_name};"
        cursor.execute(truncate_query)

        # Inserting data
        for index, row in df.iterrows():
            insert_query = f"INSERT INTO {table_name}  VALUES ('{row['post_id']}','{row['hashtags']}');"
            cursor.execute(insert_query)

        # Commit to aplly changes
        conn.commit()

        # End cursor
        cursor.close()

        # Troubleshooting
        logger.info(f'Data inserted on {table_name} successfully.')
    except psycopg2.Error as e:
        logger.error(f"Error while loading the table: {e}")
        logger.error("Additional details:")
        logger.error(f"Query executed: {insert_query}")
        logger.error(f"Data to be inserted: {df.to_dict(orient='records')}")

