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
# Função para escrever as vendas na planilha
# ============================================================
def update_sheet_30_days():
    sales = get_sales_last_30_days()
    if not sales:
        print("🚫 Nenhuma venda encontrada nos últimos 30 dias.")
        return

    # Limpa a aba antes de escrever os dados
    sheet.clear()
    print("📝 Planilha limpa antes de escrever dados atualizados.")

    # Cabeçalhos - Agora inclui TODOS os campos da API
    headers = [
        "ID Venda",
        "Transação",
        "Produto",
        "Valor",
        "Valor Líquido",
        "Taxas",
        "Cupom",
        "Lucro EAD",
        "Nome Afiliado",
        "Lucro Afiliado",
        "Data Transação",
        "Data Conclusão",
        "Tipo Pagamento",
        "Status Transação",
        "Aluno ID",
        "Nome Aluno",
        "Email",
        "Faturamento Nome",
        "Faturamento Email",
        "Faturamento Documento",
        "Faturamento Telefone",
        "Faturamento Endereço",
        "Faturamento Número",
        "Faturamento Complemento",
        "Faturamento Bairro",
        "Faturamento CEP",
        "Faturamento Cidade",
        "Faturamento UF",
        "Vendedor ID",
        "Nome Vendedor",
        "Tipo Venda",
        "Gateway",
        "Origem",
        "UTMs URL"
    ]
    sheet.append_row(headers)

    # Monta as linhas com todos os campos
    rows = []
    for sale in sales:
        rows.append([
            sale.get("vendas_id", ""),
            sale.get("transacao_id", ""),
            sale.get("produto_id", ""),
            sale.get("valor", ""),
            sale.get("valor_liquido", ""),
            sale.get("taxas", ""),
            sale.get("cupom", ""),
            sale.get("lucro_ead", ""),
            sale.get("nome_afiliado", ""),
            sale.get("lucro_afiliado", ""),
            sale.get("data_transacao", ""),
            sale.get("data_conclusao", ""),
            sale.get("tipo_pagamento", ""),
            sale.get("status_transacao", ""),
            sale.get("aluno_id", ""),
            sale.get("nome_aluno", ""),
            sale.get("email", ""),
            sale.get("faturamento_nome", ""),
            sale.get("faturamento_email", ""),
            sale.get("faturamento_documento", ""),
            sale.get("faturamento_telefone", ""),
            sale.get("faturamento_endereco", ""),
            sale.get("faturamento_numero", ""),
            sale.get("faturamento_complemento", ""),
            sale.get("faturamento_bairro", ""),
            sale.get("faturamento_cep", ""),
            sale.get("faturamento_cidade", ""),
            sale.get("faturamento_uf", ""),
            sale.get("vendedor_id", ""),
            sale.get("nome_vendedor", ""),
            sale.get("tipo_venda", ""),
            sale.get("gateway", ""),
            sale.get("origem", ""),
            sale.get("utms_url", "")
        ])

    # Escreve tudo de uma vez (evita limite de cota)
    sheet.append_rows(rows)
    print(f"✅ {len(rows)} vendas dos últimos 30 dias adicionadas à planilha!")

# ============================================================
# Execução automática a cada 30 minutos
# ============================================================
if __name__ == "__main__":
    while True:
        print("🔄 Atualizando planilha...")
        update_sheet_30_days()
        print("⏳ Aguardando 30 minutos para a próxima atualização...")
        time.sleep(1800)  # Espera 1800 segundos (30 minutos)
