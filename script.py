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
    limit = 10  # Reduzido para debug mais rápido
    offset = 0

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
            start_time = time.time()  # Marca o tempo antes da requisição
            response = requests.get(url, headers=headers, timeout=10)  # Timeout de 10 segundos
            end_time = time.time()  # Marca o tempo após resposta
            
            response.raise_for_status()  # Levanta erro se houver problema
            data = response.json()
            
            print(f"📩 Resposta da API recebida (Status {response.status_code}) em {end_time - start_time:.2f} segundos")
            print(f"📊 Total de registros recebidos: {len(data.get('rows', []))}")
        except requests.exceptions.Timeout:
            print("❌ ERRO: A API demorou muito para responder (Timeout de 10s). Tentando novamente...")
            time.sleep(5)  # Espera 5 segundos antes de tentar novamente
            continue  # Recomeça o loop
        except requests.exceptions.RequestException as e:
            print(f"❌ ERRO na requisição da API: {e}")
            break

        # Verifica se a resposta contém "rows"
        current_sales = data.get("rows", [])

        # Verifica se há transações
        if not current_sales:
            print("🚫 Nenhuma venda encontrada ou fim dos registros.")
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

            # Aplica os filtros corretos
            if (
                sale.get("tipo_pagamento") in [1, 2] and
                sale.get("status_transacao") == 2 and
                sale.get("gateway") == 6 and
                data_conclusao >= fourteen_days_ago  # Garante que é dos últimos 14 dias
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
        
        # Se o número de vendas retornadas for menor que o limite, interrompe
        if len(current_sales) < limit:
            print("✅ Todos os registros foram processados!")
            break

        offset += limit  # Avança para a próxima página de resultados

    return all_sales

# ============================================================
# Execução automática a cada 4 horas
# ============================================================
if __name__ == "__main__":
    while True:
        print("🔄 Iniciando atualização...")
        vendas = get_sales_last_14_days()
        print(f"✅ Total de vendas processadas: {len(vendas)}")
        print("⏳ Aguardando 4 horas para a próxima atualização...")
        time.sleep(14400)  # Espera 14400 segundos (4 horas)
