# %%
import requests
import datetime
import json
import pandas as pd
import os

# %%
def get_response(**kwargs):
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': '91b61e20-7595-4ace-9f4b-f8c574b90e60',  # Substitua pela sua chave de API
    }
    resp = requests.get(url, headers=headers, params=kwargs)
    if resp.status_code == 200:
        return resp.json()
    else:
        print(f"Erro na API: {resp.status_code}, {resp.text}")
        return None
    
# Salvar os dados
def save_data(data, format='json'):
    now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S.%f')
    base_path = 'Extração-Criptomoedas'  
    os.makedirs(f'{base_path}/{format}', exist_ok=True)  # Cria o diretório, se não existir

    if format == 'json':
        with open(f'{base_path}/json/{now}.json', 'w') as open_file:
            json.dump(data, open_file, indent=4)

    elif format == 'parquet':
        df = pd.DataFrame(data)
        df.to_parquet(f'{base_path}/parquet/{now}.parquet', index=False)

# %% Obter dados da API
response = get_response(start=1, limit=100, convert='USD')


# %% Se a resposta for válida, extrair e salvar os dados
if response:
    # Extração e conversão para DataFrame
    data = response.get("data", [])
    df = pd.DataFrame(data)
    print(df.head())

    # Salvar dados
    save_data(data, format='json')
    save_data(data, format='parquet')


# %%

data = pd.read_json(r'C:\Users\foxgh\OneDrive\Área de Trabalho\Data-Colletct\Extração-Criptomoedas\json\2024-12-26_19-25-56.473395.json')
data
# %%
# Filtrar por uma moeda específica, por exemplo, "Bitcoin"
moeda_especifica = data[data['name'] == 'Bitcoin']
print(moeda_especifica)

# %% Acessar o preço e arredondar

if not moeda_especifica.empty:
    quote_dict = moeda_especifica['quote'].iloc[0]  # Acessar o dicionário completo
    preco_bitcoin = quote_dict['USD']['price']     # Acessar o campo 'price'

    # Arredondar o preço para 2 casas decimais
    preco_arredondado = round(preco_bitcoin, 2)
    print(f"Preço do Bitcoin: {preco_arredondado}")
else:
    print("Moeda 'Bitcoin' não encontrada no DataFrame.")