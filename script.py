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
EAD_API_KEY = os.getenv("EAD_API_KEY")                # Chave da API EAD
SHEET_ID = os.getenv("SHEET_ID")                      # ID da planilha do Google Sheets
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")  # Conteúdo do credenciais.json

# Limite de iterações para evitar loop infinito
MAX_ITERACOES = 100

# ============================================================
# 2) AUTENTICAÇÃO NO GOOGLE SHEETS
# ============================================================
print("🔑 Autenticando no Google Sheets...")

if not GOOGLE_CREDENTIALS_JSON:
    raise ValueError("❌ ERRO: A variável de ambiente GOOGLE_CREDENTIALS_JSON está vazia ou não foi definida!")

try:
    # Carrega as credenciais a partir da string JSON
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    # Abre a planilha pela ID
    sheet = client.open_by_key(SHEET_ID).sheet1
    print("✅ Conexão com Google Sheets estabelecida!")
except Exception as e:
    raise ValueError(f"❌ ERRO ao conectar ao Google Sheets: {e}")

# ============================================================
# 3) FUNÇÃO PARA BUSCAR E FILTRAR TRANSAÇÕES (ÚLTIMOS 14 DIAS)
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

    # Loop de paginação, limitado a MAX_ITERACOES
    while iteracoes < MAX_ITERACOES:
        print(f"📌 [Iteração {iteracoes+1}/{MAX_ITERACOES}] Buscando vendas com OFFSET {offset}...")

        # Monta a URL com parâmetros
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

        print(f"🌐 Consultando API: {url}")

        try:
            start_time = time.time()
            response = requests.get(url, headers=headers, timeout=10)
            end_time = time.time()
            
            response.raise_for_status()  # Se status != 200, gera exceção
            data = response.json()
            
            print(f"📩 Resposta da API recebida (Status {response.status_code}) em {end_time - start_time:.2f} segundos")
            print(f"📊 Total de registros recebidos: {len(data.get('rows', []))}")
        except requests.exceptions.Timeout:
            print("❌ ERRO: A API demorou muito para responder. Tentando novamente em 5s...")
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

        # Filtra vendas válidas
        filtered_sales = []
        for sale in current_sales:
            data_conclusao_str = sale.get("data_conclusao")

            # Ignora vendas sem `data_conclusao`
            if not data_conclusao_str:
                total_ignoradas += 1
                continue

            # Tenta converter a data_conclusao
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
        print(f"✅ OFFSET {offset} → Vendas filtradas nesta página: {len(filtered_sales)}")

        # Se a quantidade retornada for menor que o limit, significa que chegamos ao fim
        if len(current_sales) < limit:
            print("✅ Todos os registros foram processados!")
            break

        offset += limit
        iteracoes += 1

    # Resumo final
    print(f"🔍 Resumo da execução:")
    print(f"   ✅ Total de vendas filtradas: {len(all_sales)}")
    print(f"   ⚠️ Vendas ignoradas (sem data_conclusao ou inválidas): {total_ignoradas}")

    return all_sales

# ============================================================
# 4) FUNÇÃO PARA ATUALIZAR O GOOGLE SHEETS (ordenando a data)
# ============================================================
def update_google_sheets(sales_data):
    """
    - Limpa a planilha e insere as vendas filtradas.
    - Ordena as vendas pela data_conclusao (coluna 5, índice 4).
    - Se quiser manter histórico, basta remover o sheet.clear().
    """
    print("📊 Atualizando planilha do Google Sheets...")

    # 1) Ordena a lista de vendas pela data_conclusao (índice 4)
    #    Da mais recente (reverse=True) para a mais antiga.
    sales_data.sort(
        key=lambda row: datetime.datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S"),
        reverse=True
    )

    try:
        # Limpa a planilha (remova se quiser manter histórico)
        sheet.clear()

        # Cria cabeçalhos
        headers = [
            "vendas_id", "transacao_id", "produto_id", "valor_liquido", "data_conclusao",
            "tipo_pagamento", "status_transacao", "aluno_id", "nome", "email", "gateway"
        ]
        sheet.append_row(headers)

        # Adiciona as vendas
        if sales_data:
            sheet.append_rows(sales_data)
            print(f"✅ {len(sales_data)} vendas adicionadas à planilha (ordenadas por data)!")
        else:
            print("⚠️ Nenhuma venda para adicionar na planilha.")
    except Exception as e:
        print(f"❌ ERRO ao atualizar o Google Sheets: {e}")

# ============================================================
# 5) EXECUÇÃO DO SCRIPT
# ============================================================
if __name__ == "__main__":
    print("🚀 Iniciando execução do script...")

    # Busca as vendas filtradas
    vendas_filtradas = get_sales_last_14_days()

    # Se houverem vendas, atualiza a planilha
    if vendas_filtradas:
        update_google_sheets(vendas_filtradas)
    else:
        print("⚠️ Nenhuma venda válida encontrada nos últimos 14 dias.")

    print("✅ Script finalizado com sucesso!")
