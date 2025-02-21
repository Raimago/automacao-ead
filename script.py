import os
import json
import requests
import datetime
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials

# ============================================================
# Configurações da API e da planilha
# ============================================================
EAD_API_URL = "https://ead.conhecimentointegrado.com.br/api/1/sales"
EAD_API_KEY = os.getenv("EAD_API_KEY")         # Chave da API
SHEET_ID = os.getenv("SHEET_ID")               # ID da planilha do Google Sheets
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")  # Credenciais JSON

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
# Função para buscar as transações dos últimos 14 dias
# ============================================================
def get_sales_last_14_days():
    all_sales = []
    limit = 1000
    offset = 0

    # Calcula data de hoje e 14 dias atrás
    today = datetime.date.today()
    data_fim = today.strftime("%Y-%m-%d")
    data_inicio = (today - datetime.timedelta(days=14)).strftime("%Y-%m-%d")

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

        # Exibe a estrutura exata da API (apenas na primeira iteração)
        if offset == 0:
            print("📌 Estrutura da API:")
            print(json.dumps(data, indent=4, ensure_ascii=False))

        # Verifica se a resposta contém "data"
        if not isinstance(data, dict) or "data" not in data:
            print(f"⚠️ ERRO: Resposta inesperada da API → {data}")
            break

        current_sales = data["data"]

        # Verifica se há transações e se são do tipo dicionário
        if not isinstance(current_sales, list) or not current_sales:
            print("🚫 Nenhuma venda encontrada ou estrutura inválida.")
            break

        filtered_sales = [sale for sale in current_sales if isinstance(sale, dict)]
        all_sales.extend(filtered_sales)

        print(f"📌 OFFSET {offset} → Recebidas {len(filtered_sales)} vendas.")
        offset += limit

    # Ordenação segura para evitar erros
    all_sales.sort(key=lambda x: x.get("data_transacao", ""), reverse=True)

    return all_sales

# ============================================================
# Função para limpar registros antigos e adicionar novos
# ============================================================
def update_sheet_14_days():
    sales = get_sales_last_14_days()
    if not sales:
        print("🚫 Nenhuma venda encontrada nos últimos 14 dias.")
        return

    # Lê a planilha atual
    existing_data = sheet.get_all_values()
    existing_headers = existing_data[0] if existing_data else []
    existing_rows = existing_data[1:] if len(existing_data) > 1 else []

    # Se a planilha estiver vazia, cria cabeçalhos
    if not existing_headers:
        headers = [
            "Transação ID",
            "Valor Pago",
            "Lucro EAD",
            "Data Transação",
            "Tipo Pagamento",
            "Nome",
            "Email",
            "Gateway",
            "Tipo Venda",
            "Produto ID"
        ]
        sheet.append_row(headers)

    # Converte os dados existentes para dicionário para facilitar a comparação
    existing_transactions = {row[0]: row for row in existing_rows}  # Transação ID como chave

    # Cria lista para atualização
    rows_to_update = []
    for sale in sales:
        transacao_id = sale.get("transacao_id", "")
        
        # Se a transação já estiver na planilha, não adiciona
        if transacao_id in existing_transactions:
            continue

        rows_to_update.append([
            transacao_id,
            sale.get("valor_pago", ""),
            sale.get("lucro_ead", ""),
            sale.get("data_transacao", ""),
            sale.get("tipo_pagamento", ""),
            sale.get("nome", ""),
            sale.get("email", ""),
            sale.get("gateway", ""),
            sale.get("tipo_venda", ""),
            sale.get("produto_id", "")
        ])

    # Adiciona novos registros
    if rows_to_update:
        sheet.append_rows(rows_to_update)
        print(f"✅ {len(rows_to_update)} novas vendas adicionadas!")

    # 🔥 Remove transações mais antigas que 14 dias 🔥
    fourteen_days_ago = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime("%Y-%m-%d %H:%M:%S")
    
    filtered_existing_rows = [
        row for row in existing_rows if row[3] >= fourteen_days_ago  # Verifica `data_transacao`
    ]

    # Se houverem transações antigas, atualiza a planilha sem elas
    if len(filtered_existing_rows) != len(existing_rows):
        print("🗑️ Removendo transações mais antigas que 14 dias...")
        sheet.clear()
        sheet.append_row(existing_headers)
        sheet.append_rows(filtered_existing_rows)
        print("✅ Planilha atualizada sem transações antigas!")

# ============================================================
# Execução automática a cada 4 horas
# ============================================================
if __name__ == "__main__":
    while True:
        print("🔄 Atualizando planilha...")
        update_sheet_14_days()
        print("⏳ Aguardando 4 horas para a próxima atualização...")
        time.sleep(14400)  # Espera 14400 segundos (4 horas)
