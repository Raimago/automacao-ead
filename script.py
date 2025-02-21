import os
import json
import requests
import datetime
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ============================================================
# 1) CARREGANDO VARIÁVEIS DE AMBIENTE
# ============================================================
EAD_API_URL = "https://ead.conhecimentointegrado.com.br/api/1/sales"
EAD_API_KEY = os.getenv("EAD_API_KEY")  # Chave da API EAD
SHEET_ID = os.getenv("SHEET_ID")  # ID da planilha do Google Sheets
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")  # Conteúdo do credenciais.json

# Verifica se as variáveis de ambiente estão definidas
if not EAD_API_KEY or not SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
    raise ValueError("❌ ERRO: Variáveis de ambiente não definidas!")

# ============================================================
# 2) AUTENTICAÇÃO NO GOOGLE SHEETS
# ============================================================
print("🔑 Autenticando no Google Sheets...")

try:
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).sheet1
    print("✅ Conexão com Google Sheets estabelecida!")
except Exception as e:
    raise ValueError(f"❌ ERRO ao conectar ao Google Sheets: {e}")

# ============================================================
# 3) FUNÇÃO PARA BUSCAR TRANSAÇÕES DE UM DIA ESPECÍFICO
# ============================================================
def fetch_transactions_for_day(day_str, tipo_pagamento=[1, 2], status_transacao=2, gateway=6):
    """
    Busca transações de um dia específico usando data_transacao como parâmetro.
    """
    day_sales = []
    limit = 100  # Aumentei o limite para buscar mais registros por página
    offset = 0

    while True:
        url = (
            f"{EAD_API_URL}?paginate=1"
            f"&limit={limit}"
            f"&offset={offset}"
            f"&data_inicio={day_str}"
            f"&data_fim={day_str}"
            f"&order_by=data_transacao&sort=asc"
        )
        headers = {
            "x-auth-token": EAD_API_KEY,
            "accept": "application/json"
        }
        print(f"🌐 [Dia {day_str}] Consultando API com OFFSET {offset}...")

        try:
            response = requests.get(url, headers=headers, timeout=30)  # Aumentei o timeout
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"❌ Erro ao buscar dados para {day_str}: {e}")
            break

        current_sales = data.get("rows", [])
        print(f"📊 [Dia {day_str}] Registros recebidos: {len(current_sales)}")

        if not current_sales:
            break

        for sale in current_sales:
            data_transacao_str = sale.get("data_transacao")
            if not data_transacao_str:
                continue

            try:
                datetime.datetime.strptime(data_transacao_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue

            # Aplica os filtros personalizáveis
            if (sale.get("tipo_pagamento") in tipo_pagamento and
                sale.get("status_transacao") == status_transacao and
                sale.get("gateway") == gateway):
                day_sales.append([
                    sale.get("vendas_id"),
                    sale.get("transacao_id"),
                    sale.get("produto_id"),
                    sale.get("valor_liquido"),
                    sale.get("data_transacao"),  # Usando data_transacao aqui
                    sale.get("tipo_pagamento"),
                    sale.get("status_transacao"),
                    sale.get("aluno_id"),
                    sale.get("nome"),
                    sale.get("email"),
                    sale.get("gateway"),
                ])

        if len(current_sales) < limit:
            break

        offset += limit
        time.sleep(1)  # Delay para evitar sobrecarregar a API

    return day_sales

# ============================================================
# 4) FUNÇÃO PARA BUSCAR TRANSAÇÕES DOS ÚLTIMOS 14 DIAS (DIA A DIA)
# ============================================================
def get_sales_last_14_days_by_day(tipo_pagamento=[1, 2], status_transacao=2, gateway=6):
    """
    Busca transações dos últimos 14 dias, dia a dia.
    """
    all_sales = []
    today = datetime.datetime.now()
    fourteen_days_ago = today - datetime.timedelta(days=14)

    current_date = fourteen_days_ago
    while current_date <= today:
        day_str = current_date.strftime("%Y-%m-%d")
        print(f"📆 Processando dia: {day_str}")
        day_sales = fetch_transactions_for_day(day_str, tipo_pagamento, status_transacao, gateway)
        print(f"✅ {len(day_sales)} transações encontradas para {day_str}")
        all_sales.extend(day_sales)
        current_date += datetime.timedelta(days=1)

    return all_sales

# ============================================================
# 5) FUNÇÃO PARA ATUALIZAR O GOOGLE SHEETS (ordenando por data)
# ============================================================
def update_google_sheets(sales_data):
    """
    Atualiza a planilha do Google Sheets com as transações filtradas.
    """
    print("📊 Atualizando planilha do Google Sheets...")

    # Ordena as vendas pela data_transacao (coluna 5, índice 4) em ordem cronológica
    sales_data.sort(key=lambda row: datetime.datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S"))

    try:
        sheet.clear()  # Limpa a planilha antes de adicionar novos dados
        headers = [
            "vendas_id", "transacao_id", "produto_id", "valor_liquido", "data_transacao",
            "tipo_pagamento", "status_transacao", "aluno_id", "nome", "email", "gateway"
        ]
        sheet.append_row(headers)  # Adiciona os cabeçalhos

        if sales_data:
            sheet.append_rows(sales_data)  # Adiciona as transações
            print(f"✅ {len(sales_data)} transações adicionadas à planilha (ordenadas por data)!")
        else:
            print("⚠️ Nenhuma transação para adicionar na planilha.")
    except Exception as e:
        print(f"❌ ERRO ao atualizar o Google Sheets: {e}")

# ============================================================
# 6) EXECUÇÃO DO SCRIPT
# ============================================================
if __name__ == "__main__":
    print("🚀 Iniciando execução do script...")
    vendas_filtradas = get_sales_last_14_days_by_day()  # Busca transações dos últimos 14 dias

    if vendas_filtradas:
        update_google_sheets(vendas_filtradas)  # Atualiza a planilha
    else:
        print("⚠️ Nenhuma transação válida encontrada nos últimos 14 dias.")

    print("✅ Script finalizado com sucesso!")
