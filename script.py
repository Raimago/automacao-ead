import requests
import datetime
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==============================
# CONFIGURAÇÕES DA API E GOOGLE SHEETS
# ==============================
EAD_API_URL = "https://ead.conhecimentointegrado.com.br/api/1/sales"
EAD_API_KEY = "SUA_CHAVE_AQUI"  # 🔥 Substitua pela chave real
SHEET_ID = "SEU_SHEET_ID_AQUI"  # 🔥 Substitua pelo ID real
MAX_ITERACOES = 100  # 🔥 Define um número máximo de chamadas à API

# ==============================
# AUTENTICAÇÃO NO GOOGLE SHEETS
# ==============================
print("🔑 Autenticando no Google Sheets...")

try:
    creds = ServiceAccountCredentials.from_json_keyfile_name("credenciais.json", ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).sheet1
    print("✅ Conexão com Google Sheets estabelecida!")
except Exception as e:
    print(f"❌ ERRO: Falha ao conectar ao Google Sheets: {e}")
    exit()

# ============================================================
# FUNÇÃO PARA BUSCAR E FILTRAR TRANSAÇÕES VÁLIDAS (ÚLTIMOS 14 DIAS)
# ============================================================
def get_sales_last_14_days():
    print("🔍 Iniciando busca de vendas nos últimos 14 dias...")

    all_sales = []
    limit = 10
    offset = 0
    total_ignoradas = 0
    iteracoes = 0

    # Calcula a data de hoje e 14 dias atrás
    today = datetime.datetime.now()
    fourteen_days_ago = today - datetime.timedelta(days=14)

    # Formata datas para a API
    data_inicio = fourteen_days_ago.strftime("%Y-%m-%d")
    data_fim = today.strftime("%Y-%m-%d")

    while iteracoes < MAX_ITERACOES:
        print(f"📌 Buscando vendas com OFFSET {offset} (Iteração {iteracoes + 1}/{MAX_ITERACOES})...")
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
        print(f"✅ OFFSET {offset} → Vendas filtradas: {len(filtered_sales)}")

        if len(current_sales) < limit:
            print("✅ Todos os registros foram processados!")
            break

        offset += limit
        iteracoes += 1  # 🔥 Incrementa o contador de iterações

    print(f"🔍 Resumo da execução:")
    print(f"   ✅ Total de vendas filtradas: {len(all_sales)}")
    print(f"   ⚠️ Vendas ignoradas (sem data_conclusao ou inválidas): {total_ignoradas}")

    return all_sales

# ============================================================
# FUNÇÃO PARA ATUALIZAR O GOOGLE SHEETS
# ============================================================
def update_google_sheets(sales_data):
    print("📊 Atualizando planilha do Google Sheets...")
    try:
        sheet.clear()  # Limpa a planilha antes de atualizar
        sheet.append_row(["vendas_id", "transacao_id", "produto_id", "valor_liquido", "data_conclusao",
                          "tipo_pagamento", "status_transacao", "aluno_id", "nome", "email", "gateway"])
        for row in sales_data:
            sheet.append_row(row)
        print("✅ Planilha atualizada com sucesso!")
    except Exception as e:
        print(f"❌ ERRO ao atualizar o Google Sheets: {e}")

# ==============================
# EXECUÇÃO DO SCRIPT
# ==============================
print("🚀 Iniciando execução do script...")
sales_data = get_sales_last_14_days()

if sales_data:
    update_google_sheets(sales_data)
else:
    print("⚠️ Nenhuma venda válida encontrada nos últimos 14 dias.")

print("✅ Script finalizado com sucesso!")
