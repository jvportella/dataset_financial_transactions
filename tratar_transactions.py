"""
Este script, assim como o seu semelhante, tem a funcao de remover duplicatas do arquivo csv e detectar e eliminar outliers.
Ele foi utilizado para criar uma versao "clean" do arquivo csv "transactions_data", com dados mais confiáveis e aptos 
a serem transformados em informacoes de valor.
Entretanto, optei por tornar este um arquivo separado por conta do algoritmo um pouco diferente. Apos analise manual e
analise via pgadmin, detectei que as colunas "errors" e "use_chip" nao adicionavam valor algum ao banco, e por isso
decidi remove-las.
A coluna "erros", em sua grande maioria, continha valores nulos.
A coluna "use_chip" possui apenas duas entradas, "Swipe Transaction" ou "Online Transaction", e ambas significam que
nenhum chip foi utilizado, o que implica em toda a coluna ser preenchida com "NO" (isso pode ser comprovado via query).
Portanto, ambas sao retiradas do arquivo csv "clean".
"""

import pandas as pd

# Caminho do arquivo de transações
transactions_path = 'transactions_data.csv'

# Ler o arquivo CSV
transactions_data = pd.read_csv(transactions_path)

# Função para remover duplicatas e identificar inconsistências
def clean_transactions(df):
    # Remover duplicatas
    df_cleaned = df.drop_duplicates()

    # Identificar inconsistências
    inconsistencies = {}

    # Validar colunas
    if 'id' in df_cleaned.columns:
        duplicate_ids = df_cleaned[df_cleaned['id'].duplicated(keep=False)]
        inconsistencies['duplicate_ids'] = duplicate_ids['id'].tolist()
    
    if 'amount' in df_cleaned.columns:
        invalid_amounts = df_cleaned[pd.to_numeric(df_cleaned['amount'], errors='coerce').isna()]
        inconsistencies['invalid_amounts'] = invalid_amounts['amount'].tolist()
    
    if 'use_chip' in df_cleaned.columns:
        invalid_chip_values = df_cleaned[~df_cleaned['use_chip'].isin(['YES', 'NO'])]
        inconsistencies['invalid_use_chip'] = invalid_chip_values['use_chip'].tolist()
    
    if 'date' in df_cleaned.columns:
        invalid_dates = df_cleaned[pd.to_datetime(df_cleaned['date'], errors='coerce').isna()]
        inconsistencies['invalid_dates'] = invalid_dates['date'].tolist()

    return df_cleaned, inconsistencies

# Limpar os dados e identificar inconsistências
transactions_cleaned, transactions_issues = clean_transactions(transactions_data)

# Remover as tabelas "errors" e "use_chip"
if 'errors' in transactions_cleaned.columns:
    transactions_cleaned = transactions_cleaned.drop(columns=['errors'])
if 'use_chip' in transactions_cleaned.columns:
    transactions_cleaned = transactions_cleaned.drop(columns=['use_chip'])

# Salvar o arquivo limpo
cleaned_path = 'transactions_data_cleaned.csv'
transactions_cleaned.to_csv(cleaned_path, index=False)

# Exibir resultados
print("Arquivo limpo salvo em:", cleaned_path)
print("Inconsistências encontradas:", transactions_issues)
