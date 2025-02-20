import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Configurações
API_KEY = "54993e8a003c7ffd9952fb14a46e848e"
SHEET_ID = "1mLLCMV1kZfoGSb2aLTt3Ebgq3foX6tA2M84UGogMlBs"
EAD_API_URL = "https://ead.conhecimentointegrado.com.br/api/doc/v1"

# Autenticação Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credenciais.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).sheet1

# Busca as transações
def get_transactions():
    headers = {"Authorization": f"Bearer {API_KEY}"}
    response = requests.get(EAD_API_URL, headers=headers)
    return response.json()["data"] if response.status_code == 200 else []

# Atualiza a planilha
def update_sheet():
    transactions = get_transactions()
    for transaction in transactions:
        sheet.append_row([transaction["id"], transaction["data"], transaction["valor"], transaction["status"], transaction["cliente"]["nome"]])
    print("Planilha atualizada!")

if __name__ == "__main__":
    update_sheet()
