import os
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 🌐 Configuração da API e Planilha
EAD_API_URL = "https://ead.conhecimentointegrado.com.br/api/1/sales"
EAD_API_KEY = os.getenv("EAD_API_KEY")
SHEET_ID = os.getenv("SHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

# 🚀 1. Verificação das credenciais
if not GOOGLE_CREDENTIALS_JSON:
    raise ValueError("🔴 ERRO: A variável GOOGLE_CREDENTIALS_JSON está vazia! Verifique os Secrets no GitHub.")

# 📂 2. Autenticação na API do Google Sheets
try:
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).sheet1
except Exception as e:
    raise ValueError(f"🔴 ERRO: Falha ao conectar ao Google Sheets! {e}")

# 🔄 3. Função para buscar transações da API
def get_transactions():
    headers = {
        "accept": "application/json",
        "x-auth-token": EAD_API_KEY
    }
    try:
        response = requests.get(EAD_API_URL, headers=headers)
        if response.status_code == 200:
            transactions = response.json()
            if not transactions:
                print("⚠️ Nenhuma transação encontrada.")
            return transactions
        else:
            print(f"🔴 ERRO ao buscar dados: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"🔴 ERRO: Falha ao conectar à API: {e}")
        return []

# 📝 4. Função para escrever os dados na planilha
def update_sheet():
    transactions = get_transactions()
    if not transactions:
        print("⚠️ Nenhuma transação encontrada para escrever na planilha.")
        return

    # ✅ Adiciona cabeçalhos apenas se a planilha estiver vazia
    if not sheet.get_all_values():
        headers = ["ID Venda", "ID Transação", "Produto", "Valor Pago", "Valor Líquido", "Taxas", "Cupom", "Comissão Professor"]
        sheet.append_row(headers)

    # 🗂️ Converte os dados para lista antes de escrever
    rows = []
    for transaction in transactions:
        rows.append([
            transaction.get("vendas_id", ""),
            transaction.get("transacao_id", ""),
            transaction.get("produto_id", ""),
            transaction.get("valor_pago", ""),
            transaction.get("valor_liquido", ""),
            transaction.get("taxas", ""),
            transaction.get("cupom", ""),
            transaction.get("comissao_professor", "")
        ])

    # ✅ Escreve todas as transações em um único lote (evita erro de cota)
    try:
        sheet.append_rows(rows)
        print(f"✅ {len(rows)} transações adicionadas à planilha!")
    except Exception as e:
        print(f"🔴 ERRO ao escrever na planilha: {e}")

# 🚀 Executa o script
if __name__ == "__main__":
    update_sheet()
