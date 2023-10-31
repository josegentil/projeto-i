import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import execute_values
from pymongo import MongoClient


# Conectar e transformar o banco em dataframe
conexao_mongo = MongoClient('localhost', 27017)
db = conexao_mongo['ecommerce']
colecao_db = db['order_reviews']
mydb = colecao_db.mydb
df_mongo = pd.DataFrame(list(mydb.find()))

# Extração de dados

df_customers = pd.read_csv("input\olist_customers_dataset.csv")
df_items = pd.read_csv("input\olist_order_items_dataset.csv")
df_payments = pd.read_csv("input\olist_order_payments_dataset.csv")
df_orders = pd.read_csv("input\olist_orders_dataset.csv")
df_products = pd.read_csv("input\olist_products_dataset.csv")

# Transformação dos dados


def etl_dw(fact_final: pd.DataFrame):
    juncao_fatos = df_orders.merge(df_items,
                                   left_on='order_id',
                                   right_on='order_id',
                                   how='left')

    juncao_fatos.drop(columns=['order_status',
                               'order_approved_at',
                               'order_delivered_carrier_date',
                               'order_delivered_customer_date',
                               'order_estimated_delivery_date',
                               'order_item_id',
                               'shipping_limit_date',
                               'seller_id'], inplace=True)

    juncao_fatos2 = juncao_fatos.merge(df_payments,
                                       left_on='order_id',
                                       right_on='order_id',
                                       how='left')

    juncao_fatos2.drop(columns=[
        'payment_sequential',
        'payment_type'
    ], inplace=True)

    juncao_fatos2.order_purchase_timestamp = pd.to_datetime(
        juncao_fatos2.order_purchase_timestamp)

    fact_final = juncao_fatos2.merge(df_mongo,
                                     left_on='order_id',
                                     right_on='order_id',
                                     how='left')

    fact_final.drop(columns=[
        'review_id',
        'review_comment_title',
        'review_comment_message',
        'review_creation_date',
        'review_answer_timestamp'
    ], inplace=True)

    df_payments.drop(columns=['payment_installments',
                     'payment_value'], inplace=True)

    return fact_final


def criardb_postgress():

    conexao = psycopg2.connect(
        host='localhost', password='1234', database='postgres')
    pgcursor = conexao.cursor()
    conexao.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    pgcursor.execute('DROP DATABASE IF NOT EXISTS pb_dw')
    pgcursor.execute('CREATE DATABASE IF NOT EXISTS pb_dw')
    conexao.commit()

    conexao.close


def criar_tabelas():

    pgconexao = psycopg2.connect(
        host='localhost', password='1234', database='pb_dw')
    pgconexao.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    pgcursor = pgconexao.cursor()

    pgcursor.execute('SELECT current_database()')
    pgcursor.fetchone()
    pgcursor.execute('DROP TABLE IF EXISTS pb_table')

    criacao_tabelas = \
        ("""
            CREATE TABLE IF NOT EXISTS dim_payment (
                order_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                payment_type VARCHAR(50) NOT NULL,
                payment_sequential INT
                )
            
            CREATE TABLE IF NOT EXISTS dim_customer (   
                customer_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                customer_unique_id varchar(50),
                customer_zip_code_prefix INT,
                city varchar(30) NOT NULL,
                state varchar(5) NOT NULL
                )
            
            CREATE TABLE IF NOT EXISTS dim_product (
                product_id VARCHAR(50),
                category_name VARCHAR(50),
                name_length INT,
                description_length INT,
                photos_qty INT,
                weight_g INT,
                length_cm INT,
                height_cm INT,
                width_cm INT
                )
            
            CREATE TABLE IF NOT EXISTS fact_order (
                customer_id INT FOREIGN KEY REFERENCES dim_customer (customer_id),
                product_id INT FOREIGN KEY REFERENCES dim_product (product_id),
                order_id INT FOREIGN KEY REFERENCES dim_payment (order_id),
                review_score INT,
                price DECIMAL(19, 2),
                payment_value FLOAT,
                payment_installments INT,
                freight_value DECIMAL(19, 2),
                order_purchase_timestamp DATETIME,
                )

            ALTER TABLE fact_order
            ADD CONSTRAINT FK_fact_order_dim_payment
            FOREIGN KEY (order_id)
            REFERENCES dim_payment (order_id);
            
            ALTER TABLE fact_order
            ADD CONSTRAINT FK_fact_order_dim_customer
            FOREIGN KEY (customer_id)
            REFERENCES dim_customer (customer_id);
            
            ALTER TABLE fact_order
            ADD CONSTRAINT FK_fact_order_dim_product
            FOREIGN KEY (product_id)
            REFERENCES dim_product (product_id);

    """)

    try:
        for ddl in criacao_tabelas:
            pgcursor.execute(ddl)
            pgcursor.close()
            pgconexao.commit()
            print("Tabelas criadas.")
    except psycopg2.DatabaseError as error:
        print(error)

    finally:
        if pgconexao:
            pgconexao.close()


# Carregamento dos dados

def carregar_dados(fact_final: pd.DataFrame):

    conexao = psycopg2.connect(host="localhost", port=5432,
                               database="pb_dw", user="zezo", password="zezo")
    conexao.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    executar = conexao.cursor()

    inserir_payments = f"""insert into dim_payment(order_id,
                                                    payment_sequential,
                                                    payment_type
                                                    ) values %s """

    execute_values(executar, inserir_payments, df_payments.values)

    inserir_customers = f"""insert into dim_customer(customer_id,
                                                    customer_unique_id,
                                                    customer_zip_code_prefix,
                                                    city,
                                                    state
                                                    ) values %s """

    execute_values(executar, inserir_customers, df_customers.values)

    inserir_product = f"""insert into dim_product(product_id,
                                                    category_name,
                                                    name_length,
                                                    description_length,
                                                    photos_qty,
                                                    weight_g,
                                                    length_cm,
                                                    height_cm,
                                                    width_cm
                                                    ) values %s """

    execute_values(executar, inserir_product, df_products.values)

    inserir_fact = f"""insert into fact_order(customer_id,
                                                product_id,
                                                order_id,
                                                review_score,
                                                price,
                                                payment_value,
                                                payment_installments,
                                                freight_value,
                                                order_purchase_timestamp,
                                                ) values %s """
    execute_values(executar, inserir_fact, fact_final.values)

    conexao.commit()
    executar.execute('SELECT * FROM pb_table')
    for comando in executar.fetchall():
        print(comando)
    executar.execute('TRUNCATE pb_table RESTART IDENTITY')
    conexao.commit()
    conexao.close()
    