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
    headers = {"Authorization": f"Bearer {API_KEY}"}
    response = requests.get(EAD_API_URL, headers=headers)

    if response.status_code == 200:
        return response.json().get("data", [])
    else:
        print(f"Erro na API: {response.status_code} - {response.text}")
        return []

# 🔹 Função para atualizar a planilha
def update_sheet():
    transactions = get_transactions()

    if transactions:
        for transaction in transactions:
            sheet.append_row([
                transaction.get("id"),
                transaction.get("data"),
                transaction.get("valor"),
                transaction.get("status"),
                transaction["cliente"].get("nome")
            ])
        print("✅ Planilha atualizada!")
    else:
        print("⚠️ Nenhuma transação encontrada.")

# 🔹 Executar o script
if __name__ == "__main__":
    update_sheet()

