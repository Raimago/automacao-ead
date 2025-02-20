import os
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 🔹 Carregar credenciais do GitHub Secrets
google_credentials = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not google_credentials:
    raise ValueError("❌ ERRO: A variável GOOGLE_CREDENTIALS_JSON está vazia! Verifique os Secrets no GitHub.")

creds_dict = json.loads(google_credentials)
# 🔹 Autenticação no Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(os.getenv("SHEET_ID")).sheet1

# 🔹 Configuração da API EAD
EAD_API_URL = "https://ead.conhecimentointegrado.com.br/api/v1/transacoes"
API_KEY = os.getenv("EAD_API_KEY")

# 🔹 Função para buscar transações da API
def get_transactions():
    headers = {
        "x-auth-token": API_KEY,
        "accept": "application/json"
    }
    
    all_transactions = []
    page = 1  # Começamos pela página 1

    while True:
        url = f"{EAD_API_URL}?page={page}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            try:
                data = response.json()
                if not data:
                    break  # Se a resposta estiver vazia, não há mais páginas

                all_transactions.extend(data)  # Adiciona os dados da página atual
                page += 1  # Passa para a próxima página

            except json.JSONDecodeError:
                print("⚠️ ERRO: A API retornou uma resposta inválida.")
                print("Resposta da API:", response.text)
                break
        else:
            print(f"❌ ERRO: API retornou código {response.status_code}.")
            print("Resposta da API:", response.text)
            break

    return all_transactions
