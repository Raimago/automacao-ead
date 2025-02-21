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
    limit = 10
    offset = 0
    total_ignoradas = 0

    # Calcula a data de hoje e 14 dias atrás
    today = datetime.datetime.now()
    fourteen_days_ago = today - datetime.timedelta(days=14)

    # Formata datas para a API
    data_inicio = fourteen_days_ago.strftime("%Y-%m-%d")
    data_fim = today.strftime("%Y-%m-%d")

    while True:
        url = f"{EAD_API_URL}?paginate=1&limit={limit}&offset={offset}&data_inicio={data_inicio}&data_fim={data_fim}"

        headers = {
            "x-auth-token": EAD_API_KEY,
            "accept": "application/json"
        }

        print(f"🌐 Consultando API: {url}")

        try:
            start_time = time.time()
            response = requests.get(url, headers=headers, timeout=10)
            end_time = time.time()
            
            response.raise_for_status()
            data = response.json()
            
            print(f"📩 Resposta da API recebida (Status {response.status_code}) em {end_time - start_time:.2f} segundos")
            print(f"📊 Total de registros recebidos: {len(data.get('rows', []))}")
        except requests.exceptions.Timeout:
            print("❌ ERRO: A API demorou muito para responder. Tentando novamente...")
            time.sleep(5)
            continue
        except requests.exceptions.RequestException as e:
            print(f"❌ ERRO na requisição da API: {e}")
            break

        # Verifica se a resposta contém "rows"
        current_sales = data.get("rows", [])

        if not current_sales:
            print("🚫 Nenhuma venda encontrada ou fim dos registros.")
            break

        filtered_sales = []
        for sale in current_sales:
            data_conclusao_str = sale.get("data_conclusao")

            # Ignorar vendas sem `data_conclusao`
            if not data_conclusao_str:
                total_ignoradas += 1
                continue

            try:
                data_conclusao = datetime.datetime.strptime(data_conclusao_str, "%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                total_ignoradas += 1
                continue

            # Aplica os filtros
            if (
                sale.get("tipo_pagamento") in [1, 2] and
                sale.get("status_transacao") == 2 and
                sale.get("gateway") == 6 and
                data_conclusao >= fourteen_days_ago
            ):
                filtered_sales.append([
                    sale.get("vendas_id"),
                    sale.get("transacao_id"),
                    sale.get("produto_id"),
                    sale.get("valor_liquido"),
                    sale.get("data_conclusao"),
                    sale.get("tipo_pagamento"),
                    sale.get("status_transacao"),
                    sale.get("aluno_id"),
                    sale.get("nome"),
                    sale.get("email"),
                    sale.get("gateway"),
                ])

        all_sales.extend(filtered_sales)
        print(f"📌 OFFSET {offset} → Vendas filtradas: {len(filtered_sales)}")

        if len(current_sales) < limit:
            print("✅ Todos os registros foram processados!")
            break

        offset += limit

    print(f"🔍 Resumo da execução:")
    print(f"   ✅ Total de vendas filtradas: {len(all_sales)}")
    print(f"   ⚠️ Vendas ignoradas (sem data_conclusao ou inválidas): {total_ignoradas}")

    return all_sales

# ============================================================
# Função para atualizar o Google Sheets
# ============================================================
def update_sheet_14_days():
    sales = get_sales_last_14_days()
    if not sales:
        print("🚫 Nenhuma venda válida encontrada nos últimos 14 dias.")
        return

    # Lê a planilha atual
    existing_data = sheet.get_all_values()
    existing_headers = existing_data[0] if existing_data else []

    # Se a planilha estiver vazia, cria cabeçalhos
    if not existing_headers:
        headers = [
            "Vendas ID", "Transação ID", "Produto ID", "Valor Líquido", "Data Conclusão",
            "Tipo Pagamento", "Status Transação", "Aluno ID", "Nome", "Email", "Gateway"
        ]
        sheet.append_row(headers)

    # Adiciona novas vendas
    sheet.append_rows(sales)
    print(f"✅ {len(sales)} novas vendas adicionadas ao Google Sheets!")

# ============================================================
# Execução automática a cada 4 horas
# ============================================================
if __name__ == "__main__":
    while True:
        print("🔄 Iniciando atualização...")
        update_sheet_14_days()
        print("⏳ Aguardando 4 horas para a próxima atualização...")
        time.sleep(14400)
