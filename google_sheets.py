from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
import requests
import random
import time
def load_sheet_to_df(spreadsheet_id: str, range_name: str, credentials_path: str) -> pd.DataFrame:
    """
    Загружает данные из Google Sheets в pandas DataFrame
    
    Args:
        spreadsheet_id (str): ID таблицы (можно получить из URL)
        range_name (str): Диапазон или название листа (например, 'Лист1' или 'Лист1!A1:D10')
        credentials_path (str): Путь к файлу с учетными данными сервисного аккаунта
        
    Returns:
        pd.DataFrame: DataFrame с данными из таблицы
    """
    # Загружаем учетные данные
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    
    # Создаем сервис для работы с Google Sheets API
    service = build('sheets', 'v4', credentials=credentials)
    
    # Получаем данные
    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name
    ).execute()
    
    # Преобразуем в DataFrame
    values = result.get('values', [])
    if not values:
        raise ValueError('Данные не найдены в указанном диапазоне')
        
    df = pd.DataFrame(values[1:], columns=values[0])
    return df

def filter_promo_data(df: pd.DataFrame, start_date: str, end_date: str, category: str, project: list, geo: str, subcategory: str = None, exact_start_date: bool = False, exact_end_date: bool = False) -> pd.DataFrame:
    """
    Фильтрует DataFrame по периоду, категории и проекту
    
    Args:
        df (pd.DataFrame): Исходный DataFrame
        start_date (str): Начало периода в формате DD.MM.YYYY
        end_date (str): Конец периода в формате DD.MM.YYYY
        category (str): Название категории
        project (list): Список названий проектов
        geo (str): Код гео (RU, KZ, UA, CA, DE, AU, BR)
        subcategory (str, optional): Название подкатегории (используется только если category='КАТЕГОРИЯ')
        exact_start_date (bool, optional): Если True, фильтрует записи где дата "Старт промо" точно равна start_date
        exact_end_date (bool, optional): Если True, фильтрует записи где дата "Завершение промо" точно равна end_date
        
    Returns:
        pd.DataFrame: Отфильтрованный DataFrame
    """
    # Проверяем валидность гео
    valid_geos = ['RU', 'KZ', 'UA', 'CA', 'DE', 'AU', 'BR', 'PL', 'PT','CH', 'AT']
    if geo not in valid_geos:
        raise ValueError(f"Неверное значение geo. Допустимые значения: {', '.join(valid_geos)}")
    
    # Создаем копию DataFrame для безопасности
    df = df.copy()
    
    # Заменяем None и пустые строки на NaN
    df = df.replace({None: pd.NA, '': pd.NA})
    
    # Определяем колонки для проверки на пустые значения
    required_columns = ['Старт промо', 'Завершение промо', 'Категория', 'Проект', 'Игра', 'Провайдер']
    if category == 'КАТЕГОРИЯ':
        if subcategory is None:
            raise ValueError("Для категории 'КАТЕГОРИЯ' необходимо указать параметр subcategory")
        required_columns.append('Название категории')
    
    # Удаляем строки с пустыми значениями в обязательных колонках
    df = df.dropna(subset=required_columns)
    
    # Удаляем строки с пустыми значениями в колонке 'Год' и преобразуем в числовой формат
    if 'Год' in df.columns:
        # Преобразуем колонку 'Год' в числовой формат, невалидные значения станут NaN
        df['Год'] = pd.to_numeric(df['Год'], errors='coerce')
        # Удаляем строки с NaN в колонке 'Год'
        df = df.dropna(subset=['Год'])
        # Удаляем строки с 2024 годом и меньше
        df = df[df['Год'] > 2024]
    
    # Конвертируем даты запроса
    query_start = datetime.strptime(start_date, '%d.%m.%Y')
    query_end = datetime.strptime(end_date, '%d.%m.%Y')
    
    # Конвертируем даты промо
    df['Старт промо'] = pd.to_datetime(df['Старт промо'], format='%d.%m.%Y')
    df['Завершение промо'] = pd.to_datetime(df['Завершение промо'], format='%d.%m.%Y')
    
    # Фильтруем по периоду в зависимости от параметров exact_start_date и exact_end_date
    if exact_start_date and exact_end_date:
        # Обе кнопки активны: точное совпадение и начальной, и конечной даты
        date_mask = (
            (df['Старт промо'] == query_start) & 
            (df['Завершение промо'] == query_end)
        )
    elif exact_start_date and not exact_end_date:
        # Активна только кнопка для начальной даты: точное совпадение начальной даты
        date_mask = (df['Старт промо'] == query_start)
    elif not exact_start_date and exact_end_date:
        # Активна только кнопка для конечной даты: точное совпадение конечной даты
        date_mask = (df['Завершение промо'] == query_end)
    else:
        # Обе кнопки неактивны: стандартная фильтрация по пересечению периодов
        date_mask = (
            (df['Старт промо'] <= query_end) & 
            (df['Завершение промо'] >= query_start)
        )
    
    # Фильтруем по категории и подкатегории если нужно
    if category == 'КАТЕГОРИЯ':
        category_mask = (df['Категория'] == category) & (df['Название категории'] == subcategory)
    else:
        category_mask = (df['Категория'] == category)
    
    # Создаем маску для проектов
    if project:  # Если список проектов не пустой
        project_mask = df['Проект'].str.lower().str.contains('all')
        
        # Добавляем маски для каждого проекта из списка
        for proj in project:
            project_mask |= (
                df['Проект'].str.contains(proj) |  # Точное совпадение
                df['Проект'].str.contains(f"{proj},") |  # Проект в начале списка
                df['Проект'].str.contains(f", {proj},") |  # Проект в середине списка
                df['Проект'].str.contains(f", {proj}$")  # Проект в конце списка
            )
    else:  # Если список проектов пустой, не фильтруем по проектам
        project_mask = pd.Series([True] * len(df), index=df.index)
    
    # Применяем все фильтры
    result_df = df[date_mask & category_mask & project_mask].copy()
    
    # Формируем итоговый датафрейм
    final_df = pd.DataFrame()
    final_df['Игра'] = result_df['Игра']
    final_df['Провайдер'] = result_df['Провайдер']
    final_df['Период'] = result_df['Старт промо'].dt.strftime('%d.%m.%Y') + ' - ' + result_df['Завершение промо'].dt.strftime('%d.%m.%Y')
    final_df['Проекты'] = result_df['Проект']
    final_df['Категория'] = result_df['Категория']
    
    # Определяем позицию в зависимости от категории и гео
    if category == 'КАТЕГОРИЯ' or category == 'НОВИНКИ':
        final_df['Позиция'] = result_df['Позиция']
    else:
        final_df['Позиция'] = result_df[geo]
        
    # Переупорядочиваем колонки в нужном порядке
    final_df = final_df[['Позиция', 'Игра', 'Провайдер', 'Период', 'Проекты', 'Категория']]
    
    # Сортируем по позиции
    final_df['Позиция'] = pd.to_numeric(final_df['Позиция'], errors='coerce')
    final_df = final_df.sort_values('Позиция')
    
    # Сбрасываем индекс и перемещаем его в конец
    final_df = final_df.reset_index(drop=True)
    
    return final_df
def search_game_by_nam_back(df: pd.DataFrame,project:str,geo:str) -> pd.DataFrame:
    link_project = {
    'ROX':'roxcasino',
    'SOL':'solcasino',
    'JET':'jetcasino',
    'FRESH':'freshcasino',
    'IZZI':'izzicasino',
    'Legzo':'legzocasino',
    'STARDA':'stardacasino',
    'DRIP':'dripcasino',
    'Monro':'monrocasino',
    '1GO':'1gocasino',
    'LEX':'lex-casino',
    'Gizbo':'gizbocasino',
    'Irwin':'irwincasino',
    'FLAGMAN':'flagmancasino'   
}
    data_game = send_request(project,link_project)
    if data_game:
        # Создаем новую колонку 'Игра на бэке'
        def get_game_backend(game_name):
            game_name_lower = game_name.lower().replace(' ', '').strip()
            for key, value in data_game.items():
                if key.lower().replace(' ', '').strip() == game_name_lower:
                    return value.lower()
            return '-'
            
        df.insert(1, 'id на бэке', df['Игра'].apply(get_game_backend))
        return df
    else:
        return df
mirror_cache = {}
mirror_cache_timeout = 3600
def send_request(project,link_project):
    if project not in link_project:
        return []
    
    # Проверяем кэш
    current_time = time.time()
    if project in mirror_cache:
        cache_time, mirror, result = mirror_cache[project]
        if current_time - cache_time < mirror_cache_timeout:
            return result
    
    base_domain = link_project[project]
    url_template = f"https://{base_domain}{{mirror}}.com/batch?cms[]=api/cms/v2/games/RUB"
    
    # Диапазоны зеркал для разных проектов
    mirror_ranges = {
        'stardacasino': range(100, 1001),
        'monrocasino': range(100, 1001),
        '1gocasino': range(100, 1001),
        'gizbocasino': range(788, 797),
        'irwincasino': range(1, 10),
        'flagmancasino': range(1, 10)
    }
    
    # Выбор диапазона или стандартный вариант
    mirror_range = mirror_ranges.get(base_domain, range(1000, 10000))
    
    # Перемешиваем возможные зеркала для более эффективной проверки
    mirrors = list(mirror_range)
    random.shuffle(mirrors)
    
    timeout = 3  # Таймаут в секундах
    
    for mirror in mirrors:
        full_url = url_template.format(mirror=mirror)
        try:
            proxies = {
                'http': 'http://vpn@vpn548794202.opengw.net:1465',
                'https': 'https://vpn@vpn548794202.opengw.net:1465'
            }
            response = requests.get(full_url, timeout=timeout, proxies=proxies)
            
            if response.status_code == 200:
                json_data = response.json()
                game_dict = {}
                
                for game_id, game_data in json_data.get('CmsApiCmsV2GamesRUB', {}).get('data', {}).items():
                    game_dict[game_data.get('title')] = game_data.get('identifier')
                return game_dict                 
                
        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Ошибка при запросе к {full_url}: {e}")
            continue
    
    print(f"Не удалось найти рабочее зеркало для проекта {project}")
    return []

def get_logs_by_id(spreadsheet_id: str, credentials_path: str, date_column: str = "Дата", limit: int = None) -> pd.DataFrame:
    """
    Получает все логи из листа 'логи', сортирует по дате (самые новые сверху) и оставляет только записи не старше 3 месяцев.
    Args:
        spreadsheet_id (str): ID таблицы
        credentials_path (str): Путь к credentials.json
        date_column (str): Название колонки с датой (по умолчанию 'Дата')
        limit (int, optional): Ограничение количества записей для ускорения загрузки
    Returns:
        pd.DataFrame: DataFrame с логами, отсортированный по дате (убывание) и не старше 3 месяцев
    """
    df_logs = load_sheet_to_df(spreadsheet_id, "логи", credentials_path)
    
    # Проверяем, что данные не пустые
    if df_logs.empty:
        return df_logs
    
    # Преобразуем столбец с датой к datetime для корректной сортировки и фильтрации
    df_logs[date_column] = pd.to_datetime(df_logs[date_column], errors='coerce', dayfirst=True)
    
    # Удаляем строки с некорректными датами
    df_logs = df_logs.dropna(subset=[date_column])
    
    # Оставляем только записи не старше 3 месяцев
    three_months_ago = pd.Timestamp.now() - pd.DateOffset(months=3)
    df_logs = df_logs[df_logs[date_column] >= three_months_ago]
    
    # Сортируем по дате (самые новые сверху)
    df_logs = df_logs.sort_values(date_column, ascending=False)
    
    # Ограничиваем количество записей для ускорения (если указано)
    if limit and len(df_logs) > limit:
        df_logs = df_logs.head(limit)
    
    df_logs = df_logs.reset_index(drop=True)
    return df_logs


if __name__ == '__main__':
    SPREADSHEET_ID = '1m7TE_YFLtf2opgral3YVr7SeJk2BSh7YXuWtEUDUcNY'
    RANGE_NAME = 'Сводный'
    CREDENTIALS_PATH = 'credentials.json'
    
    # Загружаем данные
    df = load_sheet_to_df(SPREADSHEET_ID, RANGE_NAME, CREDENTIALS_PATH)
    
    # Фильтруем данные (пример)
    filtered_df = filter_promo_data(
        df,
        start_date='30.12.2024',
        end_date='05.01.2025',
        category='КАТЕГОРИЯ',
        project=['ROX'],
        geo='RU',
        subcategory='Fruits',
        exact_start_date=False,
        exact_end_date=False
    )
    
    print("\nРезультат фильтрации:")
    print(filtered_df) 