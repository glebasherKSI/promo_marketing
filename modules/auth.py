import streamlit as st
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd


SPREADSHEET_ID = '1Ka9yKchuEQaqPIE1KpsGIT8Yf5IjjTTPkLckgnuoIQs'
RANGE_NAME = 'main'
CREDENTIALS_PATH = 'credentials.json'
# Проверка, авторизован ли пользователь
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

# Простая функция аутентификации (в реальности используйте хеширование!)
def authenticate(username, password):
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH,
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    
    # Создаем сервис для работы с Google Sheets API
    service = build('sheets', 'v4', credentials=credentials)
    
    # Получаем данные
    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME
    ).execute()
    
    # Преобразуем в DataFrame
    values = result.get('values', [])
    if not values:
        raise ValueError('Данные не найдены в указанном диапазоне')
        
    df = pd.DataFrame(values[1:], columns=values[0])
    print(df)
    # Проверяем наличие пользователя в датафрейме
    user_row = df[df['user'] == username]
    if user_row.empty:
        return False
        
    # Получаем пароль из датафрейма и сравниваем
    stored_password = user_row['password'].iloc[0]
    return stored_password == password

def show_auth_page():
    # Интерфейс формы
    st.title("🔐 Вход в систему")

    # Проверяем сохраненные данные
    if "saved_username" not in st.session_state:
        st.session_state.saved_username = ""
    if "saved_password" not in st.session_state:
        st.session_state.saved_password = ""

    username = st.text_input("Логин", value=st.session_state.saved_username, key="login_username")
    password = st.text_input("Пароль", type="password", value=st.session_state.saved_password, key="login_password")

    # Создаем состояние для отслеживания процесса авторизации
    if "auth_in_progress" not in st.session_state:
        st.session_state.auth_in_progress = False

    # Кнопка входа с индикатором загрузки
    if st.button("Войти", disabled=st.session_state.auth_in_progress):
        st.session_state.auth_in_progress = True
        with st.spinner("Проверка учетных данных..."):
            if authenticate(username, password):
                # Сохраняем данные для следующего входа
                st.session_state.saved_username = username
                st.session_state.saved_password = password
                st.session_state.logged_in = True
                st.success("✅ Вы успешно вошли!")
                st.rerun()
            else:
                st.error("❌ Неверный логин или пароль")
                st.session_state.auth_in_progress = False