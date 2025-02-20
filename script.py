import os
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 🔹 Carrega as credenciais da API do Google Sheets
creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON", "{}"))
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
client = gspread.authorize(creds)

# 🔹 Configurações da API EAD e Google Sheets
API_KEY = os.environ.get("EAD_API_KEY", "")
SHEET_ID = os.environ.get("SHEET_ID", "")
EAD_API_URL = "https://ead.conhecimentointegrado.com.br/api/1/sales"
HEADERS = {
    "accept": "application/json",
    "x-auth-token": API_KEY
}

# 🔹 Conectar ao Google Sheets
sheet = client.open_by_key(SHEET_ID).sheet1

# 🔹 Função para buscar transações
def get_transactions():
    response = requests.get(EAD_API_URL, headers=HEADERS)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Erro ao buscar dados: {response.status_code}")
        return []

# 🔹 Função para salvar dados na planilha
def update_sheet():
    transactions = get_transactions()

    if not transactions:
        print("Nenhuma transação encontrada.")
        return

    # Adiciona cabeçalho se a planilha estiver vazia
    if not sheet.get_all_values():
        sheet.append_row(["ID Venda", "ID Transação", "Produto", "Valor Pago", "Valor Líquido", "Taxas", "Cupom", "Comissão Professor"])

    # Prepara os dados para escrita em lote
    data_to_write = []
    for transaction in transactions:
        data_to_write.append([
            transaction.get("vendas_id", ""),
            transaction.get("transacao_id", ""),
            transaction.get("produto_id", ""),
            transaction.get("valor_pago", ""),
            transaction.get("valor_liquido", ""),
            transaction.get("taxas", ""),
            transaction.get("cupom", ""),
            transaction.get("comissao_professor", "")
        ])
    
    # Evita múltiplas requisições à API do Google Sheets
    sheet.append_rows(data_to_write)
    print(f"✅ {len(data_to_write)} transações adicionadas à planilha!")

# 🔹 Executar o script
if __name__ == "__main__":
    update_sheet()
