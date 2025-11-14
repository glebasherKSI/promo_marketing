import streamlit as st
import importlib
import sys
import os
import logging

# Добавляем путь к каталогу с проектом, чтобы можно было импортировать модули
sys.path.append(os.path.dirname(__file__))

# Настраиваем логирование для подавления WebSocket ошибок
# Устанавливаем уровень ERROR для всех tornado логгеров
tornado_loggers = [
    "tornado.access",
    "tornado.application", 
    "tornado.general",
    "tornado.websocket",
    "tornado.iostream"
]

# Подавляем ошибки WebSocketClosedError
class WebSocketErrorFilter(logging.Filter):
    def filter(self, record):
        # Фильтруем сообщения об ошибках WebSocket
        message = str(record.getMessage())
        if any(keyword in message for keyword in [
            "WebSocketClosedError",
            "Stream is closed",
            "StreamClosedError",
            "Task exception was never retrieved"
        ]):
            return False
        # Фильтруем по имени исключения
        if record.exc_info:
            exc_type = record.exc_info[0]
            if exc_type and "WebSocketClosedError" in str(exc_type):
                return False
        return True

# Применяем фильтр и настраиваем уровень логирования
for logger_name in tornado_loggers:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.ERROR)
    logger.addFilter(WebSocketErrorFilter())
    
    # Удаляем все существующие обработчики и добавляем новый с фильтром
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Создаем новый обработчик с фильтром
    handler = logging.StreamHandler()
    handler.setLevel(logging.ERROR)
    handler.addFilter(WebSocketErrorFilter())
    logger.addHandler(handler)

# Настройки страницы
st.set_page_config(
    page_title="Управление продажами провайдеров",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Скрываем меню гамбургера и footer
hide_menu_style = """
    <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        /* Скрываем верхние кнопки навигации страниц */
        header[data-testid="stHeader"] {
            display: none !important;
        }
        /* Скрываем кнопки "Вернуться" в верхнем левом углу */
        .stApp > header {
            display: none !important;
        }
        /* Убираем лишние отступы */
        .block-container {
            padding-top: 1rem;
        }
        /* Скрываем навигацию по страницам в сайдбаре */
        [data-testid="stSidebarNav"] {
            display: none !important;
        }
        /* Скрываем верхнюю часть сайдбара с navItems */
        .css-6qob1r.e1fqkh3o3 {
            display: none !important;
        }
        /* Также скрываем другие варианты классов */
        .css-17eq0hr {
            display: none !important;
        }
        .css-uc1cuc {
            display: none !important;
        }
        .css-1aehpvj {
            display: none !important;
        }
        /* Скрываем все элементы списка навигации */
        ul[class^="css-"] {
            display: none !important;
        }
        div[data-testid="collapsedControl"] {
            display: none !important;
        }
    </style>
"""
st.markdown(hide_menu_style, unsafe_allow_html=True)

# Основная точка входа
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

# Состояние сайдбара
if "sidebar_state" not in st.session_state:
    st.session_state.sidebar_state = False

# Функция для отображения/скрытия сайдбара
def toggle_sidebar():
    if st.session_state.logged_in:
        # Показываем сайдбар, только если пользователь авторизован
        st.session_state.sidebar_state = True
        # Это хак, вставляющий CSS для показа сайдбара
        st.markdown(
            """
            <style>
            /* Показываем сайдбар */
            section[data-testid="stSidebar"] {
                display: flex !important;
                width: 250px !important;
            }
            </style>
            """,
            unsafe_allow_html=True
        )
    else:
        # Скрываем сайдбар для неавторизованных пользователей
        st.session_state.sidebar_state = False
        # Это хак, вставляющий CSS для скрытия сайдбара
        st.markdown(
            """
            <style>
            /* Скрываем сайдбар полностью */
            section[data-testid="stSidebar"] {
                display: none !important;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

# Применяем настройки сайдбара
toggle_sidebar()

# Добавим JavaScript для скрытия навигации в сайдбаре
hide_sidebar_nav_script = """
<script>
    // Функция для скрытия элементов навигации в сайдбаре
    function hideNavElements() {
        // Найдем все элементы верхней навигации в сайдбаре
        const sidebarNavs = window.parent.document.querySelectorAll('[data-testid="stSidebarNav"]');
        sidebarNavs.forEach(nav => {
            nav.style.display = 'none';
        });
        
        // Найдем все списки в сайдбаре (они содержат ссылки на страницы)
        const sidebarLists = window.parent.document.querySelectorAll('section[data-testid="stSidebar"] ul');
        sidebarLists.forEach(list => {
            list.style.display = 'none';
        });
        
        // Найдем все заголовки в сайдбаре (они обычно содержат тексты "НАВИГАЦИЯ" и т.п.)
        const sidebarHeaders = window.parent.document.querySelectorAll('section[data-testid="stSidebar"] [data-testid="stSidebarNav"] > div:first-child');
        sidebarHeaders.forEach(header => {
            header.style.display = 'none';
        });
    }
    
    // Вызовем функцию сразу и повторно через короткий интервал, чтобы поймать, когда элементы появятся
    hideNavElements();
    setTimeout(hideNavElements, 100);
    setTimeout(hideNavElements, 500);
    setTimeout(hideNavElements, 1000);
</script>
"""
st.markdown(hide_sidebar_nav_script, unsafe_allow_html=True)

# Импортируем страницы напрямую
if st.session_state.logged_in:
    # Импортируем и запускаем страницу отношений из модулей
    from modules.relations import show_page
    show_page()
else:
    # Импортируем и запускаем страницу авторизации из модулей
    from modules.auth import show_auth_page
    show_auth_page()