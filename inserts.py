""" 
Para a versao atual deste script, a versao do python eh 3.13.1 e a versao do pgadmin eh 8.11.
Em versoes desatualizadas, o psycopg3 (utilizado neste codigo) nao funciona e aponta erros no encoding utf-8.
A funcao desse script eh realizar os inserts do arquivo csv dentro do banco de dados automaticamente, ao mesmo tempo
que trata os dados antes da insercao, definindo a tipagem esperada de cada coluna e transformando dados incoerentes
para seus valores esperados.
Como no banco foi criada a tabela "merchants" mas nao ha um arquivo separado para esta tabela, ele processa os dados
e automaticamente insere apenas as colunas de interesse nessa tabela.
"""

import pandas as pd
import psycopg

# Configuração da conexão com o banco de dados
DATABASE_URI = "postgresql://postgres:1234@localhost/financial"

# Função para limpar colunas numéricas
def clean_numeric_column(df, column_name):
    """
    Remove caracteres não numéricos de uma coluna e converte para float.
    """
    df[column_name] = (
        df[column_name]
        .astype(str)
        .str.replace(r"[^\d.-]", "", regex=True)
        .astype(float)
    )

# Função para inserir dados no banco
def insert_data(table_name, columns, data, conn):
    """
    Insere dados no banco de dados PostgreSQL.
    """
    if len(data) == 0:
        print(f"Nenhum dado para inserir na tabela '{table_name}'.")
        return

    try:
        placeholders = ', '.join(['%s'] * len(columns))
        columns_formatted = ', '.join(columns)
        conflict_column = columns[0]
        query = f"""
            INSERT INTO {table_name} ({columns_formatted})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_column}) DO NOTHING
        """
        with conn.cursor() as cur:
            cur.executemany(query, data)
        conn.commit()  # Confirmar a transação
        print(f"Dados inseridos na tabela '{table_name}' (duplicatas ignoradas).")
    except psycopg.Error as e:
        print(f"Erro ao inserir dados na tabela '{table_name}': {e}")
        raise

# Caminhos dos arquivos CSV limpos
users_csv = 'users_data_cleaned.csv'
cards_csv = 'cards_data_cleaned.csv'
transactions_csv = 'transactions_data_cleaned.csv'

# Estabelecer conexão
try:
    conn = psycopg.connect(DATABASE_URI)

    # Inserir dados na tabela "users"
    users_data = pd.read_csv(users_csv)
    users_columns = ['id', 'current_age', 'retirement_age', 'birth_year', 'birth_month',
                     'gender', 'address', 'latitude', 'longitude', 'per_capita_income',
                     'yearly_income', 'total_debt', 'credit_score', 'num_credit_cards']
    users_data = users_data[users_columns]  # Selecionar colunas relevantes
    print(f"Registros na tabela 'users': {len(users_data)}")
    insert_data('users', users_columns, users_data.values.tolist(), conn)

    # Inserir dados na tabela "cards"
    cards_data = pd.read_csv(cards_csv)
    cards_columns = ['id', 'client_id', 'card_brand', 'card_type', 'expires', 'cvv',
                     'has_chip', 'num_cards_issued', 'credit_limit', 'acct_open_date',
                     'year_pin_last_changed', 'card_on_dark_web']
    cards_data = cards_data[cards_columns]  # Selecionar colunas relevantes
    print(f"Registros na tabela 'cards': {len(cards_data)}")
    insert_data('cards', cards_columns, cards_data.values.tolist(), conn)

    # Processar e inserir dados na tabela "merchants"
    transactions_data = pd.read_csv(transactions_csv)
    merchants_data = transactions_data[['merchant_id', 'merchant_city', 'merchant_state', 'zip', 'mcc']].drop_duplicates()
    merchants_data = merchants_data.rename(columns={'merchant_id': 'id'})  # Renomear coluna para 'id'
    merchants_columns = ['id', 'merchant_city', 'merchant_state', 'zip', 'mcc']
    print(f"Registros na tabela 'merchants': {len(merchants_data)}")
    merchants_data = merchants_data[merchants_columns]
    insert_data('merchants', merchants_columns, merchants_data.values.tolist(), conn)

    # Processar e inserir dados na tabela "transactions"
    transactions_columns = ['id', 'date', 'client_id', 'card_id', 'merchant_id', 'amount']
    transactions_cleaned = transactions_data[transactions_columns]

    # Limpar dados
    clean_numeric_column(transactions_cleaned, 'amount')
    transactions_cleaned = transactions_cleaned.fillna({
        'id': 0,
        'date': '1970-01-01 00:00:00',
        'client_id': 0,
        'card_id': 0,
        'merchant_id': 0,
        'amount': 0.0
    })
    transactions_cleaned = transactions_cleaned.astype({
        'id': int,
        'date': str,
        'client_id': int,
        'card_id': int,
        'merchant_id': int,
        'amount': float
    })
    print(f"Registros na tabela 'transactions': {len(transactions_cleaned)}")
    insert_data('transactions', transactions_columns, transactions_cleaned.values.tolist(), conn)

except psycopg.OperationalError as e:
    print(f"Erro de conexão: {e}")
finally:
    if 'conn' in locals() and not conn.closed:
        conn.close()
        print("Conexão com o banco encerrada.")
