import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from google_sheets import load_sheet_to_df, filter_promo_data, get_logs_by_id
import io
# Константы
SPREADSHEET_ID = '1m7TE_YFLtf2opgral3YVr7SeJk2BSh7YXuWtEUDUcNY'
RANGE_NAME = 'Сводный'
CREDENTIALS_PATH = 'credentials.json'
VALID_GEOS = ['RU', 'KZ', 'UA', 'CA', 'DE', 'AU', 'BR', 'PL', 'PT','CH', 'AT' ]
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

# Загружаем логи (без автообновления, только по кнопке)
def load_logs_data(limit: int = 500):
    """
    Загружает логи с ограничением количества для ускорения работы
    Args:
        limit: Максимальное количество записей для загрузки (по умолчанию 500)
    """
    try:
        return get_logs_by_id(SPREADSHEET_ID, CREDENTIALS_PATH, limit=limit)
    except Exception as e:
        st.error(f"Ошибка при загрузке логов: {str(e)}")
        return pd.DataFrame()

# Получаем список колонок из основного листа (без автообновления)
@st.cache_data  # Кэш без TTL - обновляется только при явной очистке
def get_column_names():
    """
    Получает список названий колонок из основного листа 'Сводный'
    для использования в отображении логов изменений
    """
    try:
        df = load_sheet_to_df(SPREADSHEET_ID, RANGE_NAME, CREDENTIALS_PATH)
        # Возвращаем список названий колонок
        return df.columns.tolist()
    except Exception:
        # В случае ошибки возвращаем базовый список колонок
        # (предупреждение будет показано в show_reports_tab)
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
            ["Размещение слотов", "Логи изменений"],
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
    elif tab_selected == "Логи изменений":
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
    st.title("Логи изменений (за последние 3 месяца)")
    
    # Добавляем глобальные стили для скроллбара
    st.markdown("""
    <style>
        /* Стили для красивого скроллбара таблиц */
        .log-table-container::-webkit-scrollbar {
            height: 12px;
        }
        .log-table-container::-webkit-scrollbar-track {
            background: #f1f1f1;
            border-radius: 10px;
        }
        .log-table-container::-webkit-scrollbar-thumb {
            background: #888;
            border-radius: 10px;
        }
        .log-table-container::-webkit-scrollbar-thumb:hover {
            background: #555;
        }
        /* Для Firefox */
        .log-table-container {
            scrollbar-width: thin;
            scrollbar-color: #888 #f1f1f1;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Инициализируем session_state для хранения данных
    if 'logs_data' not in st.session_state:
        st.session_state.logs_data = None
    if 'column_names' not in st.session_state:
        st.session_state.column_names = None
    if 'logs_loading' not in st.session_state:
        st.session_state.logs_loading = False
    
    # Загружаем данные при первой загрузке
    if st.session_state.logs_data is None and not st.session_state.logs_loading:
        st.session_state.logs_loading = True
        with st.spinner('Загрузка логов...'):
            try:
                st.session_state.logs_data = load_logs_data()
                st.session_state.column_names = get_column_names()
            finally:
                st.session_state.logs_loading = False
    
    # Кнопка обновления
    col1, col2 = st.columns([1, 4])
    with col1:
        refresh = st.button('🔄 Обновить', use_container_width=True)
    
    # Обновляем данные при нажатии кнопки
    if refresh:
        st.session_state.logs_loading = True
        with st.spinner('Загрузка логов...'):
            try:
                # Очищаем кэш колонок при обновлении
                get_column_names.clear()
                # Загружаем данные
                st.session_state.logs_data = load_logs_data()
                st.session_state.column_names = get_column_names()
                st.success('✅ Логи обновлены!', icon="✅")
            except Exception as e:
                st.error(f"Ошибка при обновлении: {str(e)}")
            finally:
                st.session_state.logs_loading = False
    
    # Показываем сообщение, если данные еще загружаются
    if st.session_state.logs_loading:
        st.info("⏳ Загрузка данных...")
        return
    
    try:
        logs_df = st.session_state.logs_data
        base_cell_names = st.session_state.column_names
        
        # Проверяем, что данные загружены
        if logs_df is None:
            st.info("Нажмите кнопку '🔄 Обновить' для загрузки логов")
            return
            
        if logs_df.empty:
            st.warning("Нет логов за последние 3 месяца.")
            return
        
        # Если не удалось загрузить колонки из документа, используем базовый список
        if base_cell_names is None:
            st.warning("Не удалось загрузить список колонок из документа. Используется базовый список.")
            base_cell_names = [
                'Год', 'Статус', 'Провайдер', 'Месяц', 'Проект', 'Размещение', 'Старт промо', 'Завершение промо',
                'Игра', 'Категория', 'Позиция', 'Название категории', 'Скидка', 'ПФ (комп)', 'Период скидки',
                'RU', 'KZ', 'UA', 'CA', 'DE', 'AU', 'BR', 'Гео', 'Комменатрии'
            ]

        card_bg = '#fff'
        card_border = '#d1d5db'
        header_bg = '#e9ecef'
        table_bg = '#f8f9fa'
        border_color = '#d1d5db'
        text_color = '#222'
        subtext_color = '#555'
        old_bg = '#ffb3b3'
        new_bg = '#b3ffb3'

        for idx, row in logs_df.iterrows():
            date = row.get('Дата', '')
            # Преобразуем дату к формату 'дд.мм.гггг чч:мм' если возможно
            try:
                date_obj = pd.to_datetime(date)
                date = date_obj.strftime('%d.%m.%Y %H:%M')
            except Exception:
                pass
            user = row.get('Пользователь', '')
            cell = row.get('Ячейка', '')
            old = str(row.get('Старое значение', ''))
            new = str(row.get('Новое значение', ''))

            old_cells = [x.strip() for x in old.split('|')]
            new_cells = [x.strip() for x in new.split('|')]

            max_len = max(len(old_cells), len(new_cells), len(base_cell_names))
            cell_names = base_cell_names + [''] * (max_len - len(base_cell_names))
            old_cells += [''] * (max_len - len(old_cells))
            new_cells += [''] * (max_len - len(new_cells))

            # Создаем таблицу с фиксированной минимальной шириной для ячеек
            table_html = f"<table style='min-width:100%;width:max-content;border-collapse:collapse;background:{table_bg};color:{text_color};white-space:nowrap;'>"
            # Первая строка — заголовки
            table_html += f"<tr><th style='padding:8px 12px;background:{header_bg};color:{subtext_color};border:1px solid {border_color};font-size:0.9em;position:sticky;left:0;z-index:10;min-width:120px;'>{'Тип'}</th>"
            for name in cell_names:
                table_html += f"<th style='padding:8px 12px;background:{header_bg};color:{subtext_color};border:1px solid {border_color};font-size:0.9em;min-width:100px;'>{name}</th>"
            table_html += "</tr>"
            # Вторая строка — старое значение
            table_html += "<tr>"
            table_html += f"<td style='background:{header_bg};color:{subtext_color};font-weight:bold;text-align:right;padding:8px 12px;border:1px solid {border_color};position:sticky;left:0;z-index:5;min-width:120px;'>Старое значение</td>"
            for i, val in enumerate(old_cells):
                # Ограничиваем длину текста для лучшего отображения
                display_val = str(val)[:50] + ('...' if len(str(val)) > 50 else '')
                if val != new_cells[i]:
                    table_html += f"<td style='background:{old_bg};color:#111;padding:8px 12px;font-family:monospace;border:1px solid {border_color};min-width:100px;' title='{val}'>{display_val}</td>"
                else:
                    table_html += f"<td style='padding:8px 12px;font-family:monospace;color:{subtext_color};border:1px solid {border_color};min-width:100px;'>{display_val}</td>"
            table_html += "</tr>"
            # Третья строка — новое значение
            table_html += "<tr>"
            table_html += f"<td style='background:{header_bg};color:{subtext_color};font-weight:bold;text-align:right;padding:8px 12px;border:1px solid {border_color};position:sticky;left:0;z-index:5;min-width:120px;'>Новое значение</td>"
            for i, val in enumerate(new_cells):
                # Ограничиваем длину текста для лучшего отображения
                display_val = str(val)[:50] + ('...' if len(str(val)) > 50 else '')
                if val != old_cells[i]:
                    table_html += f"<td style='background:{new_bg};color:#111;padding:8px 12px;font-family:monospace;border:1px solid {border_color};min-width:100px;' title='{val}'>{display_val}</td>"
                else:
                    table_html += f"<td style='padding:8px 12px;font-family:monospace;color:{subtext_color};border:1px solid {border_color};min-width:100px;'>{display_val}</td>"
            table_html += "</tr>"
            table_html += "</table>"

            st.markdown(f"""
            <div style='background:{card_bg};padding:1.2em 1.5em;margin-bottom:2em;border-radius:10px;border:2px solid {card_border};box-shadow:0 2px 4px rgba(0,0,0,0.1);'>
                <div style='color:{subtext_color};font-size:0.95em;margin-bottom:1em;padding-bottom:0.5em;border-bottom:1px solid {border_color};'>🕒 <b>{date}</b> &nbsp; 👤 <b>{user}</b> &nbsp; <span style='color:{subtext_color}'>Ячейка:</span> <b>{cell}</b></div>
                <div class='log-table-container' style='overflow-x:auto;overflow-y:visible;width:100%;max-width:100%;border:1px solid {border_color};border-radius:5px;'>
                    {table_html}
                </div>
            </div>
            """, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Ошибка при загрузке логов: {str(e)}")

def show_settings_tab():
    st.title("⚙️ Настройки")
    st.info("Раздел настроек в разработке") 