"""
Este script basicamente tem a funcao de remover duplicatas do arquivo csv e detectar e eliminar outliers.
Ele foi utilizado para criar uma versao "clean" dos arquivos csv "cards_data" e "users_data", com dados
mais confiáveis e aptos a serem transformados em informacoes de valor.
"""

import pandas as pd

# Caminhos dos arquivos
cards_data_path = 'cards_data.csv'
users_data_path = 'users_data.csv'

# Ler os arquivos CSV
cards_data = pd.read_csv(cards_data_path)
users_data = pd.read_csv(users_data_path)

# Função para remover duplicatas e identificar inconsistências
def clean_and_identify_issues(df, column_types):
    # Remover duplicatas
    df_cleaned = df.drop_duplicates()
    
    # Identificar valores inconsistentes
    issues = {}
    for column, expected_type in column_types.items():
        if column in df_cleaned.columns:
            if expected_type == 'numeric':
                # Valores não numéricos em colunas que deveriam ser numéricas
                non_numeric = df_cleaned[pd.to_numeric(df_cleaned[column], errors='coerce').isna()]
                issues[column] = non_numeric[column].tolist()
            elif expected_type == 'categorical':
                # Valores nulos ou anômalos em colunas categóricas
                null_or_empty = df_cleaned[df_cleaned[column].isna() | (df_cleaned[column] == "")]
                issues[column] = null_or_empty[column].tolist()
    
    return df_cleaned, issues

# Definir tipos esperados para as colunas de cada arquivo
cards_column_types = {
    'id': 'numeric',
    'client_id': 'numeric',
    'card_brand': 'categorical',
    'card_type': 'categorical',
    'expires': 'categorical',
    'cvv': 'numeric',
    'has_chip': 'categorical',
    'num_cards_issued': 'numeric',
    'credit_limit': 'numeric',
    'acct_open_date': 'categorical',
    'year_pin_last_changed': 'numeric',
    'card_on_dark_web': 'categorical'
}

users_column_types = {
    'id': 'numeric',
    'current_age': 'numeric',
    'retirement_age': 'numeric',
    'birth_year': 'numeric',
    'birth_month': 'numeric',
    'gender': 'categorical',
    'address': 'categorical',
    'latitude': 'numeric',
    'longitude': 'numeric',
    'per_capita_income': 'numeric',
    'yearly_income': 'numeric',
    'total_debt': 'numeric',
    'credit_score': 'numeric',
    'num_credit_cards': 'numeric'
}

# Limpar dados e identificar problemas
cards_cleaned, cards_issues = clean_and_identify_issues(cards_data, cards_column_types)
users_cleaned, users_issues = clean_and_identify_issues(users_data, users_column_types)

# Exibir os resultados
print("Cartões - Dados Limpos:", cards_cleaned.shape)
print("Cartões - Problemas Identificados:", cards_issues)

print("Usuários - Dados Limpos:", users_cleaned.shape)
print("Usuários - Problemas Identificados:", users_issues)

# Salvar os dados limpos (se necessário)
cards_cleaned.to_csv('cards_data_cleaned.csv', index=False)
users_cleaned.to_csv('users_data_cleaned.csv', index=False)
