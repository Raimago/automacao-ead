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
    print("🔑 Autenticando no Google Sheets...")
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).sheet1
    print("✅ Conexão com Google Sheets estabelecida!")
except Exception as e:
    raise ValueError(f"❌ ERRO ao conectar ao Google Sheets: {e}")

# ============================================================
# Função para buscar e filtrar as transações dos últimos 14 dias
# ============================================================
def get_sales_last_14_days():
    all_sales = []
    limit = 50  # Inicialmente limitamos para evitar travamento
    offset = 0

    # Calcula a data de hoje e 14 dias atrás
    today = datetime.datetime.now()
    fourteen_days_ago = today - datetime.timedelta(days=14)

    while True:
        url = f"{EAD_API_URL}?paginate=1&limit={limit}&offset={offset}"

        headers = {
            "x-auth-token": EAD_API_KEY,
            "accept": "application/json"
        }

        print(f"🌐 Consultando API: {url}")
        
        try:
            response = requests.get(url, headers=headers, timeout=15)  # Timeout de 15s
            response.raise_for_status()  # Verifica se a requisição teve erro
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ ERRO na requisição da API: {e}")
            break

        # Verifica se a resposta contém "data"
        if not isinstance(data, dict) or "data" not in data:
            print(f"⚠️ ERRO: Resposta inesperada da API → {data}")
            break

        current_sales = data["data"]

        # Verifica se há transações
        if not isinstance(current_sales, list) or not current_sales:
            print("🚫 Nenhuma venda encontrada ou estrutura inválida.")
            break

        # Aplica o filtro e mantém apenas os campos necessários
        filtered_sales = []
        for sale in current_sales:
            data_conclusao_str = sale.get("data_conclusao", "")

            # Verifica se a data está correta
            try:
                data_conclusao = datetime.datetime.strptime(data_conclusao_str, "%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                print(f"⚠️ Ignorando venda com data inválida: {data_conclusao_str}")
                continue  # Pula se a data estiver errada

            # Aplica os filtros
            if (
                sale.get("tipo_pagamento") in [1, 2] and
                sale.get("status_transacao") == 2 and
                sale.get("gateway") == 6 and
                data_conclusao >= fourteen_days_ago  # Filtro por data
            ):
                filtered_sales.append({
                    "vendas_id": sale.get("vendas_id"),
                    "transacao_id": sale.get("transacao_id"),
                    "produto_id": sale.get("produto_id"),
                    "valor_liquido": sale.get("valor_liquido"),
                    "data_conclusao": sale.get("data_conclusao"),
                    "tipo_pagamento": sale.get("tipo_pagamento"),
                    "status_transacao": sale.get("status_transacao"),
                    "aluno_id": sale.get("aluno_id"),
                    "nome": sale.get("nome"),
                    "email": sale.get("email"),
                    "gateway": sale.get("gateway"),
                })

        all_sales.extend(filtered_sales)
        print(f"📌 OFFSET {offset} → Vendas filtradas: {len(filtered_sales)}")
        offset += limit

        # Se nenhuma venda for retornada, interrompe o loop
        if len(filtered_sales) < limit:
            break

    return all_sales

# ============================================================
# Função para atualizar a planilha do Google Sheets
# ============================================================
def update_sheet_14_days():
    print("📄 Atualizando Google Sheets...")
    sales = get_sales_last_14_days()

    if not sales:
        print("🚫 Nenhuma venda encontrada nos últimos 14 dias.")
        return

    print(f"🔢 Total de vendas filtradas: {len(sales)}")

    # Lê a planilha atual
    existing_data = sheet.get_all_values()
    existing_headers = existing_data[0] if existing_data else []
    existing_rows = existing_data[1:] if len(existing_data) > 1 else []

    # Se a planilha estiver vazia, cria cabeçalhos
    if not existing_headers:
        headers = [
            "Vendas ID", "Transação ID", "Produto ID", "Valor Líquido",
            "Data Conclusão", "Tipo Pagamento", "Status Transação",
            "Aluno ID", "Nome", "Email", "Gateway"
        ]
        sheet.append_row(headers)

    # Previne duplicatas
    existing_transactions = {row[1]: row for row in existing_rows}  

    rows_to_update = []
    for sale in sales:
        transacao_id = sale.get("transacao_id", "")

        if transacao_id in existing_transactions:
            continue

        rows_to_update.append([
            sale.get("vendas_id"), sale.get("transacao_id"), sale.get("produto_id"),
            sale.get("valor_liquido"), sale.get("data_conclusao"), sale.get("tipo_pagamento"),
            sale.get("status_transacao"), sale.get("aluno_id"), sale.get("nome"),
            sale.get("email"), sale.get("gateway")
        ])

    if rows_to_update:
        sheet.append_rows(rows_to_update)
        print(f"✅ {len(rows_to_update)} novas vendas adicionadas!")

# ============================================================
# Execução automática a cada 4 horas
# ============================================================
if __name__ == "__main__":
    while True:
        print("🔄 Iniciando atualização...")
        update_sheet_14_days()
        print("⏳ Aguardando 4 horas para a próxima atualização...")
        time.sleep(14400)  # Espera 14400 segundos (4 horas)
