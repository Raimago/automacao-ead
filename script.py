import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os

# Configuração da API do Google Sheets
SHEET_ID = os.getenv("SHEET_ID")  # Pegando o ID da planilha do ambiente
API_KEY = os.getenv("EAD_API_KEY")  # Pegando a API Key do ambiente
EAD_API_URL = "https://ead.conhecimentointegrado.com.br/api/1/sales"

# Autenticação com Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
google_credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")

if not google_credentials_json:
    raise ValueError("❌ ERRO: A variável GOOGLE_CREDENTIALS_JSON está vazia! Verifique os Secrets no GitHub.")

creds_dict = json.loads(google_credentials_json)  # Convertendo JSON de string para dicionário
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).sheet1

# Busca todas as transações
def get_transactions():
    headers = {
        "x-auth-token": API_KEY,
        "Accept": "application/json"
    }
    
    response = requests.get(EAD_API_URL, headers=headers)
    
    if response.status_code == 200:
        return response.json()  # Retorna os dados das transações
    else:
        print(f"Erro na API: {response.status_code} - {response.text}")
        return []

# Atualiza a planilha com os dados da API
def update_sheet():
    transactions = get_transactions()
    
    if not transactions:
        print("Nenhuma transação encontrada.")
        return

    # Adiciona cabeçalhos se a planilha estiver vazia
    if not sheet.get_all_values():
        sheet.append_row(["ID Venda", "ID Transação", "Produto", "Valor Pago", "Valor Líquido", "Taxas", "Cupom", "Comissão Professor"])
    
    # Adiciona os dados das transações na planilha
    for transaction in transactions:
        sheet.append_row([
            transaction.get("vendas_id", ""),
            transaction.get("transacao_id", ""),
            transa
