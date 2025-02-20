import requests
import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials

# Configurações
API_KEY = "54993e8a003c7ffd9952fb14a46e848e"  # Substitua pela sua API KEY
SHEET_ID = "1mLLCMV1kZfoGSb2aLTt3Ebgq3foX6tA2M84UGogMlBs"  # Substitua pelo ID da planilha
EAD_API_URL = "https://ead.conhecimentointegrado.com.br/api/1/sales"  # URL da API

# Autenticação com Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON", "{}"))  # Pega as credenciais do GitHub Secrets
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).sheet1

# Função para buscar as transações
def get_transactions():
    headers = {
        "Accept": "application/json",
        "x-auth-token": API_KEY  # Token de autenticação
    }
    response = requests.get(EAD_API_URL, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao buscar transações: {response.status_code} - {response.text}")
        return []

# Atualiza a planilha com os dados das transações
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
            transaction.get("produto_id", ""),
            transaction.get("valor_pago", ""),
            transaction.get("valor_liquido", ""),
            transaction.get("taxas", ""),
            transaction.get("cupom", ""),
            transaction.get("comissao_professor", "")
        ])
    
    print("Planilha atualizada com sucesso!")

# Executa o script
if __name__ == "__main__":
    update_sheet()
