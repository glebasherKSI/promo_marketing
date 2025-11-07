import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from google_sheets import load_sheet_to_df, filter_promo_data
import io
import plotly.express as px
import calendar
import uuid
import json
from streamlit_javascript import st_javascript
from streamlit_echarts import st_echarts
from streamlit_echarts import JsCode
# Константы
SPREADSHEET_ID = '1m7TE_YFLtf2opgral3YVr7SeJk2BSh7YXuWtEUDUcNY'
RANGE_NAME = 'Сводный'
CREDENTIALS_PATH = 'credentials.json'
VALID_GEOS = ['RU', 'KZ', 'UA', 'CA', 'DE', 'AU', 'BR', 'PL', 'PT','CH', 'AT']
VALID_CATEGORIES = ['ГЛАВНАЯ','КАТЕГОРИЯ', 'НОВИНКИ']

# Функция для получения уникальных значений из колонки с учетом разделителей
def get_unique_values(df, column):
    if df[column].empty:
        return []
    # Объединяем все значения в одну строку
    all_values = ', '.join(df[column].dropna())
    # Разбиваем по запятой и очищаем от пробелов
    values = [x.strip() for x in all_values.split(',')]
    # Убираем дубликаты и пустые значения
    return sorted(list(set(x for x in values if x)))

# Функция для получения уникальных значений с обработкой None
def get_unique_non_null_values(series):
    # Удаляем None и пустые значения, затем берем уникальные
    return sorted([x for x in series.unique() if pd.notna(x) and x != ''])

# Кэшируем загрузку данных
@st.cache_data(ttl=600)  # Кэш на 1 час
def load_data():
    try:
        df = load_sheet_to_df(SPREADSHEET_ID, RANGE_NAME, CREDENTIALS_PATH)
        # Заменяем None и пустые строки на NaN
        df = df.replace({None: pd.NA, '': pd.NA})
        return df
    except Exception as e:
        st.error(f"Ошибка при загрузке данных: {str(e)}")
        return None

def show_page():
    # Проверка авторизации
    if "logged_in" not in st.session_state or not st.session_state.logged_in:
        st.error("⛔ Требуется авторизация!")
        st.stop()
        
    # Добавляем кнопку выхода в сайдбар
    with st.sidebar:
        st.title("⚙️ Меню")
        # Добавляем информацию о пользователе
        st.markdown("---")
        # Добавляем разделы навигации
        st.markdown("### 📋 Разделы")
        tab_selected = st.radio(
            "Выберите раздел:",
            ["Размещение слотов", "Скоро"],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        
        # Кнопка выхода в нижней части сайдбара с CSS-стилями
        exit_button_style = """
        <style>
        div[data-testid="stButton"] button {
            background-color: #dc3545;
            color: white;
            border: none;
            border-radius: 4px;
            padding: 0.5rem 1rem;
            width: 100%;
            margin-top: 20px;
        }
        div[data-testid="stButton"] button:hover {
            background-color: #c82333;
            color: black;
        }
        div[data-testid="stButton"] button:active {
            color: black;
        }
        </style>
        """
        st.markdown(exit_button_style, unsafe_allow_html=True)
        
        if st.button("🚪 Выйти из системы"):
            st.session_state.logged_in = False
            st.session_state.auth_in_progress = False
            st.rerun()
    
    # Основной контент в зависимости от выбранного раздела
    if tab_selected == "Размещение слотов":
        show_promo_tab()
    elif tab_selected == "Скоро":
        show_reports_tab()
    elif tab_selected == "Настройки":
        show_settings_tab()

def show_promo_tab():
    # Заголовок
    st.title("Размещение слотов")

    # Загружаем данные
    df = load_data()

    if df is not None:
        # Создаем колонки для размещения фильтров
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Выбор категории
            categories = get_unique_non_null_values(df['Категория'])
            category = st.selectbox('Выберите категорию', VALID_CATEGORIES)
            
            # Если выбрана КАТЕГОРИЯ, показываем выбор подкатегории
            subcategory = None
            if category == 'КАТЕГОРИЯ':
                subcategories = get_unique_non_null_values(df[df['Категория'] == 'КАТЕГОРИЯ']['Название категории'])
                subcategory = st.selectbox('Выберите подкатегорию', subcategories)
        
        with col2:
            # Выбор проектов (мультивыбор)
            projects = get_unique_values(df[df['Категория'].isin(VALID_CATEGORIES)], 'Проект')
            selected_projects = st.multiselect(
                'Выберите проекты',
                options=projects,
                help="Можно выбрать несколько проектов. Если ничего не выбрано, фильтрация по проектам не применяется"
            )
            
            print(selected_projects)
            
            # Выбор ГЕО
            geo = st.selectbox('Выберите ГЕО', VALID_GEOS)
        
        with col3:
            # Выбор периода
            st.write('Выберите период')
            
            # Режимы фильтрации для начальной даты
            filter_mode_start = st.checkbox(
                'Альтернативный режим фильтрации для НАЧАЛА периода',
                help='Учитывать только записи с датой "Старт промо" равной дате "Начало периода"'
            )
            
            # Режимы фильтрации для конечной даты
            filter_mode_end = st.checkbox(
                'Альтернативный режим фильтрации для КОНЦА периода',
                help='Учитывать только записи с датой "Завершение промо" равной дате "Конец периода"'
            )
            
            # По умолчанию устанавливаем текущий месяц
            today = datetime.now()
            start_date = st.date_input(
                'Начало периода',
                value=today.replace(day=1)
            )
            end_date = st.date_input(
                'Конец периода',
                value=(today.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            )
        
        # Кнопка для применения фильтров
        if st.button('Применить фильтры'):
            with st.spinner('Применяем фильтры...'):
                try:
                    # Показываем информацию о режиме фильтрации
                    if filter_mode_start and filter_mode_end:
                        st.info("🎯 Режим фильтрации: точное совпадение начальной И конечной даты")
                    elif filter_mode_start and not filter_mode_end:
                        st.info("📅 Режим фильтрации: точное совпадение только начальной даты")
                    elif not filter_mode_start and filter_mode_end:
                        st.info("📅 Режим фильтрации: точное совпадение только конечной даты")
                    else:
                        st.info("📊 Режим фильтрации: стандартный (пересечение периодов)")
                    
                    # Конвертируем даты в строки нужного формата
                    start_str = start_date.strftime('%d.%m.%Y')
                    end_str = end_date.strftime('%d.%m.%Y')
                    
                    # Получаем отфильтрованные данные
                    filtered_df = filter_promo_data(
                        df,
                        start_date=start_str,
                        end_date=end_str,
                        category=category,
                        project=selected_projects,  # Передаем список выбранных проектов
                        geo=geo,
                        subcategory=subcategory,
                        exact_start_date=filter_mode_start,
                        exact_end_date=filter_mode_end
                    )
                    
                    # Показываем результаты
                    if not filtered_df.empty:
                        # Удаляем строки с пустыми значениями в колонке Позиция
                        filtered_df = filtered_df.dropna(subset=['Позиция'])
                        
                        if not filtered_df.empty:
                            st.write(f"Найдено записей: {len(filtered_df)}")
                            st.dataframe(
                                filtered_df,
                                use_container_width=True,
                                column_config={
                                    "Позиция": st.column_config.NumberColumn(
                                        "Позиция",
                                        help="Позиция в выбранном ГЕО",
                                        format="%d"
                                    )
                                }
                            )
                        else:
                            st.warning('После удаления записей с пустыми позициями ничего не осталось')
                        # Создаем буфер для Excel файла
                        buffer = io.BytesIO()
                        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                            filtered_df.to_excel(writer, index=False, sheet_name='Результаты')
                            
                        # Кнопка для скачивания результатов
                        if st.download_button(
                            label="Скачать как Excel",
                            data=buffer.getvalue(),
                            file_name=f"promo_filter_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.ms-excel"
                        ):
                            st.success('Файл успешно скачан!')
                    else:
                        st.warning('По заданным фильтрам ничего не найдено')
                    
                except Exception as e:
                    st.error(f"Ошибка при фильтрации данных: {str(e)}")
    else:
        st.error("Не удалось загрузить данные. Проверьте подключение и учетные данные.")

def show_reports_tab():
    st.title("В разработке или нет...")
    

def show_settings_tab():
    st.title("⚙️ Настройки")
    st.info("Раздел настроек в разработке") 