import os
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ============================================================
# Configurações da API e da planilha
# ============================================================
EAD_API_URL = "https://ead.conhecimentointegrado.com.br/api/1/sales"
EAD_API_KEY = os.getenv("EAD_API_KEY")         # Sua chave da API (configurada como variável de ambiente ou secret)
SHEET_ID = os.getenv("SHEET_ID")               # ID da sua planilha do Google Sheets
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")  # Conteúdo do arquivo JSON das credenciais

# ============================================================
# Verificação das credenciais necessárias
# ============================================================
if not GOOGLE_CREDENTIALS_JSON:
    raise ValueError("ERRO: A variável GOOGLE_CREDENTIALS_JSON está vazia! Verifique seus Secrets.")

# ============================================================
# Autenticação no Google Sheets
# ============================================================
try:
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    # Acessa a primeira aba da planilha (você pode mudar para outra aba se necessário)
    sheet = client.open_by_key(SHEET_ID).sheet1
except Exception as e:
    raise ValueError(f"ERRO ao conectar ao Google Sheets: {e}")

# ============================================================
# Função para buscar transações da API EAD
# ============================================================
def get_transactions():
    headers = {
        "accept": "application/json",
        "x-auth-token": EAD_API_KEY
    }
    try:
        response = requests.get(EAD_API_URL, headers=headers)
        if response.status_code == 200:
            data = response.json()
            # Se os dados estiverem encapsulados na chave "data", extraímos eles
            if isinstance(data, dict) and "data" in data:
                transactions = data.get("data", [])
            else:
                transactions = data
            print("DEBUG: Dados da API:", transactions)
            return transactions
        else:
            print(f"ERRO: Código {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"ERRO ao conectar à API: {e}")
        return []

# ============================================================
# Função para atualizar a planilha com as transações
# ============================================================
def update_sheet():
    transactions = get_transactions()
    if not transactions:
        print("Nenhuma transação encontrada para atualizar a planilha.")
        return

    # Se a planilha estiver vazia, adiciona o cabeçalho
    if not sheet.get_all_values():
        cabeçalhos = ["ID Venda", "ID Transação", "Produto", "Valor Pago", "Valor Líquido", "Taxas", "Cupom", "Comissão Professor"]
        sheet.append_row(cabeçalhos)
        print("DEBUG: Cabeçalhos adicionados na planilha.")

    # Prepara os dados a serem inseridos (ajuste os nomes dos campos conforme a estrutura retornada pela API)
    linhas = []
    for transaction in transactions:
        linha = [
            transaction.get("vendas_id", ""),
            transaction.get("transacao_id", ""),
            transaction.get("produto_id", ""),
            transaction.get("valor_pago", ""),
            transaction.get("valor_liquido", ""),
            transaction.get("taxas", ""),
            transaction.get("cupom", ""),
            transaction.get("comissao_professor", "")
        ]
        linhas.append(linha)

    # Insere todas as linhas de uma vez para evitar exceder a cota de requisições
    try:
        sheet.append_rows(linhas)
        print(f"DEBUG: {len(linhas)} transações adicionadas à planilha.")
        # Lê os dados da planilha para confirmar a inserção
        all_data = sheet.get_all_values()
        print("DEBUG: Conteúdo atual da planilha:", all_data)
    except Exception as e:
        print(f"ERRO ao escrever na planilha: {e}")

# ============================================================
# Execução do script
# ============================================================
if __name__ == "__main__":
    update_sheet()
