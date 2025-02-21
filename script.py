# ============================================================
# Função para buscar e filtrar as transações dos últimos 14 dias
# ============================================================
def get_sales_last_14_days():
    all_sales = []
    limit = 1000
    offset = 0

    # Calcula a data de hoje e 14 dias atrás
    today = datetime.datetime.now()
    fourteen_days_ago = today - datetime.timedelta(days=14)

    while True:
        url = (
            f"{EAD_API_URL}?paginate=1"
            f"&limit={limit}"
            f"&offset={offset}"
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

        # Verifica se a resposta contém "data"
        if not isinstance(data, dict) or "data" not in data:
            print(f"⚠️ ERRO: Resposta inesperada da API → {data}")
            break

        current_sales = data["data"]

        # Verifica se há transações e se são do tipo dicionário
        if not isinstance(current_sales, list) or not current_sales:
            print("🚫 Nenhuma venda encontrada ou estrutura inválida.")
            break

        # Aplica o filtro e mantém apenas os campos necessários
        filtered_sales = []
        for sale in current_sales:
            data_conclusao_str = sale.get("data_conclusao", "")

            # Verifica se a data está em um formato válido
            try:
                data_conclusao = datetime.datetime.strptime(data_conclusao_str, "%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                print(f"⚠️ Ignorando venda com data inválida: {data_conclusao_str}")
                continue  # Pula esta transação se a data estiver errada

            # Aplica os filtros exigidos
            if (
                sale.get("tipo_pagamento") in [1, 2] and
                sale.get("status_transacao") == 2 and
                sale.get("gateway") == 6 and
                data_conclusao >= fourteen_days_ago  # Filtro baseado na data_conclusao
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

        print(f"📌 OFFSET {offset} → Recebidas {len(filtered_sales)} vendas após filtro.")
        offset += limit

    # Ordenação por data de conclusão (mais recentes primeiro)
    all_sales.sort(key=lambda x: x.get("data_conclusao", ""), reverse=True)

    return all_sales
