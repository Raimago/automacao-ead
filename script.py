import os
import json
import requests
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ============================================================
# Configurações da API e da planilha
# ============================================================
EAD_API_URL = "https://ead.conhecimentointegrado.com.br/api/1/sales"
EAD_API_KEY = os.getenv("EAD_API_KEY")         # Chave da API (GitHub Secret ou variável de ambiente)
SHEET_ID = os.getenv("SHEET_ID")               # ID da planilha do Google Sheets
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")  # Conteúdo JSON das credenciais

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
    sheet = client.open_by_key(SHEET_ID).sheet1
except Exception as e:
    raise ValueError(f"ERRO ao conectar ao Google Sheets: {e}")

# ============================================================
# Função para buscar TODAS as transações dos últimos 30 dias
# ============================================================
def get_sales_last_30_days():
    all_sales = []
    limit = 1000
    offset = 0

    # Calcula data de hoje e 30 dias atrás
    today = datetime.date.today()
    data_fim = today.strftime("%Y-%m-%d")
    data_inicio = (today - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

    while True:
        url = (
            f"{EAD_API_URL}?paginate=1"
            f"&limit={limit}"
            f"&offset={offset}"
            f"&data_inicio={data_inicio}"
            f"&data_fim={data_fim}"
        )

        headers = {
            "x-auth-token": EAD_API_KEY,
            "accept": "application/json"
        }

        response = requests.get(url, headers=headers)

        # Verifica se a API retornou erro
        if response.status_code != 200:
            print(f"ERRO {response.status_code}: {response.text}")
            break

        data = response.json()

        # Verifica se a resposta contém "data", caso contrário, é um erro
        if not isinstance(data, dict) or "data" not in data:
            print(f"⚠️ ERRO: Resposta inesperada da API → {data}")
            break

        current_sales = data["data"]

        # Verifica se realmente há transações
        if not isinstance(current_sales, list) or not current_sales:
            print("🚫 Nenhuma venda encontrada ou estrutura inválida.")
            break

        # Filtra apenas objetos do tipo dicionário (evita erro 'get' em strings)
        filtered_sales = [sale for sale in current_sales if isinstance(sale, dict)]
        all_sales.extend(filtered_sales)

        print(f"📌 OFFSET {offset} → Recebidas {len(filtered_sales)} vendas.")
        offset += limit

    # Ordenação segura (evita erro caso existam valores inválidos)
    all_sales.sort(key=lambda x: x.get("data_transacao", ""), reverse=True)

    return all_sales

# ============================================================
# Função para escrever as vendas na planilha
# ============================================================
def update_sheet_30_days():
    sales = get_sales_last_30_days()
    if not sales:
        print("🚫 Nenhuma venda encontrada nos últimos 30 dias.")
        return

    # Limpa a aba (opcional) para evitar duplicados
    sheet.clear()
    print("📝 Planilha limpa antes de escrever dados atualizados.")

    # Cabeçalhos
    headers = [
        "ID Venda",
        "Transação",
        "Produto",
        "Valor Pago",
        "Valor Líquido",
        "Taxas",
        "Cupom",
        "Data Transação",
        "Status",
        "Nome Aluno",
        "Email",
    ]
    sheet.append_row(headers)

    # Monta as linhas (ajuste conforme a estrutura da API)
    rows = []
    for sale in sales:
        rows.append([
            sale.get("vendas_id", ""),
            sale.get("transacao_id", ""),
            sale.get("produto_id", ""),
            sale.get("valor_pago", ""),
            sale.get("valor_liquido", ""),
            sale.get("taxas", ""),
            sale.get("cupom", ""),
            sale.get("data_transacao", ""),
            sale.get("status_transacao", ""),
            sale.get("nome_aluno", ""),
            sale.get("email", ""),
        ])

    # Escreve tudo de uma vez (evita limite de cota)
    sheet.append_rows(rows)
    print(f"✅ {len(rows)} vendas dos últimos 30 dias adicionadas à planilha!")

# ============================================================
# Execução automática a cada 30 minutos
# ============================================================
import time

if __name__ == "__main__":
    while True:
        print("🔄 Atualizando planilha...")
        update_sheet_30_days()
        print("⏳ Aguardando 30 minutos para a próxima atualização...")
        time.sleep(1800)  # Espera 1800 segundos (30 minutos)
