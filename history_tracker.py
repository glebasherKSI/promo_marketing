from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime, timedelta
import json
import os
import hashlib

def get_sheet_revision_history(spreadsheet_id: str, credentials_path: str, max_revisions: int = 50) -> pd.DataFrame:
    """
    Получает историю изменений Google Sheets файла
    
    Args:
        spreadsheet_id (str): ID таблицы
        credentials_path (str): Путь к файлу с учетными данными
        max_revisions (int): Максимальное количество ревизий для получения
        
    Returns:
        pd.DataFrame: DataFrame с историей изменений
    """
    # Загружаем учетные данные с расширенными правами
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=[
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/drive.readonly'
        ]
    )
    
    # Создаем сервис для работы с Google Drive API
    drive_service = build('drive', 'v3', credentials=credentials)
    
    try:
        # Получаем список ревизий
        revisions = drive_service.revisions().list(
            fileId=spreadsheet_id,
            fields='revisions(id,modifiedTime,lastModifyingUser,size)'
        ).execute()
        
        revision_list = []
        for revision in revisions.get('revisions', []):
            revision_data = {
                'revision_id': revision.get('id'),
                'modified_time': revision.get('modifiedTime'),
                'user_name': revision.get('lastModifyingUser', {}).get('displayName', 'Неизвестно'),
                'user_email': revision.get('lastModifyingUser', {}).get('emailAddress', 'Неизвестно'),
                'size_bytes': revision.get('size', 0)
            }
            revision_list.append(revision_data)
        
        # Преобразуем в DataFrame
        df = pd.DataFrame(revision_list)
        
        if not df.empty:
            # Конвертируем время в читаемый формат
            df['modified_time'] = pd.to_datetime(df['modified_time'])
            df['modified_time_formatted'] = df['modified_time'].dt.strftime('%d.%m.%Y %H:%M:%S')
            
            # Сортируем по времени (новые сверху)
            df = df.sort_values('modified_time', ascending=False)
            
            # Ограничиваем количество записей
            df = df.head(max_revisions)
        
        return df
        
    except Exception as e:
        print(f"Ошибка при получении истории изменений: {e}")
        return pd.DataFrame()

def track_data_changes(spreadsheet_id: str, range_name: str, credentials_path: str, tracking_file: str = 'changes_log.json') -> dict:
    """
    Отслеживает изменения в данных путем сравнения с предыдущим состоянием
    
    Args:
        spreadsheet_id (str): ID таблицы
        range_name (str): Диапазон для отслеживания
        credentials_path (str): Путь к файлу с учетными данными
        tracking_file (str): Файл для хранения истории изменений
        
    Returns:
        dict: Информация об изменениях
    """
    from google_sheets import load_sheet_to_df
    
    try:
        # Принудительно загружаем СВЕЖИЕ данные (без кэша)
        current_data = load_sheet_to_df(spreadsheet_id, range_name, credentials_path)
        
        # Очищаем данные для более надежного сравнения
        current_data = current_data.replace({None: '', pd.NA: ''}).fillna('')
        
        # Создаем стабильный хэш на основе содержимого
        current_content = current_data.to_csv(index=False)
        current_hash = hashlib.md5(current_content.encode('utf-8')).hexdigest()
        
        print(f"Загружено строк: {len(current_data)}")
        print(f"Текущий хэш: {current_hash}")
        
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        return {
            'timestamp': datetime.now().isoformat(),
            'has_changes': False,
            'error': str(e),
            'current_hash': None,
            'previous_hash': None
        }
    
    # Загружаем предыдущее состояние
    if os.path.exists(tracking_file):
        try:
            with open(tracking_file, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except Exception as e:
            print(f"Ошибка при чтении файла истории: {e}")
            history = {'changes': []}
    else:
        history = {'changes': []}
    
    # Получаем последний хэш
    last_hash = history.get('last_hash', None)
    print(f"Предыдущий хэш: {last_hash}")
    
    has_changes = current_hash != last_hash
    print(f"Обнаружены изменения: {has_changes}")
    
    change_info = {
        'timestamp': datetime.now().isoformat(),
        'has_changes': has_changes,
        'current_hash': current_hash,
        'previous_hash': last_hash,
        'rows_count': len(current_data),
        'columns_count': len(current_data.columns)
    }
    
    # Если есть изменения, записываем их
    if has_changes:
        print("Записываем изменения в историю...")
        # Добавляем запись об изменении
        history['changes'].append(change_info)
        history['last_hash'] = current_hash
        history['last_update'] = change_info['timestamp']
        
        # Ограничиваем историю (оставляем последние 100 записей)
        history['changes'] = history['changes'][-100:]
        
        # Сохраняем обновленную историю
        try:
            with open(tracking_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
            print("История успешно сохранена")
        except Exception as e:
            print(f"Ошибка при сохранении истории: {e}")
    else:
        print("Изменений не обнаружено")
    
    return change_info

def get_file_activity(spreadsheet_id: str, credentials_path: str, days_back: int = 7) -> pd.DataFrame:
    """
    Получает активность файла за указанный период
    
    Args:
        spreadsheet_id (str): ID таблицы
        credentials_path (str): Путь к файлу с учетными данными
        days_back (int): Количество дней назад для анализа
        
    Returns:
        pd.DataFrame: DataFrame с активностью файла
    """
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=[
            'https://www.googleapis.com/auth/drive.readonly'
        ]
    )
    
    drive_service = build('drive', 'v3', credentials=credentials)
    
    try:
        # Получаем общую информацию о файле
        file_info = drive_service.files().get(
            fileId=spreadsheet_id,
            fields='name,modifiedTime,lastModifyingUser,version,viewedByMe,viewedByMeTime'
        ).execute()
        
        activity_data = [{
            'activity_type': 'Последнее изменение',
            'timestamp': file_info.get('modifiedTime'),
            'user_name': file_info.get('lastModifyingUser', {}).get('displayName', 'Неизвестно'),
            'user_email': file_info.get('lastModifyingUser', {}).get('emailAddress', 'Неизвестно'),
            'file_name': file_info.get('name', 'Неизвестно'),
            'version': file_info.get('version', 'Неизвестно')
        }]
        
        # Если есть информация о просмотре
        if file_info.get('viewedByMeTime'):
            activity_data.append({
                'activity_type': 'Последний просмотр',
                'timestamp': file_info.get('viewedByMeTime'),
                'user_name': 'Я',
                'user_email': 'Текущий пользователь',
                'file_name': file_info.get('name', 'Неизвестно'),
                'version': file_info.get('version', 'Неизвестно')
            })
        
        df = pd.DataFrame(activity_data)
        
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['formatted_time'] = df['timestamp'].dt.strftime('%d.%m.%Y %H:%M:%S')
            
            # Фильтруем по времени
            time_threshold = datetime.now() - timedelta(days=days_back)
            df = df[df['timestamp'] >= time_threshold]
            
            df = df.sort_values('timestamp', ascending=False)
        
        return df
        
    except Exception as e:
        print(f"Ошибка при получении активности файла: {e}")
        return pd.DataFrame()

def get_changes_summary(tracking_file: str = 'changes_log.json') -> dict:
    """
    Получает сводку по изменениям из файла истории
    
    Args:
        tracking_file (str): Файл с историей изменений
        
    Returns:
        dict: Сводка по изменениям
    """
    if not os.path.exists(tracking_file):
        return {
            'total_changes': 0,
            'last_change': None,
            'changes_today': 0,
            'changes_this_week': 0
        }
    
    with open(tracking_file, 'r', encoding='utf-8') as f:
        history = json.load(f)
    
    changes = history.get('changes', [])
    
    if not changes:
        return {
            'total_changes': 0,
            'last_change': None,
            'changes_today': 0,
            'changes_this_week': 0
        }
    
    # Конвертируем даты
    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=7)
    
    changes_today = 0
    changes_this_week = 0
    
    for change in changes:
        change_time = datetime.fromisoformat(change['timestamp'])
        if change_time >= today_start:
            changes_today += 1
        if change_time >= week_start:
            changes_this_week += 1
    
    return {
        'total_changes': len(changes),
        'last_change': changes[-1]['timestamp'] if changes else None,
        'changes_today': changes_today,
        'changes_this_week': changes_this_week,
        'last_hash': history.get('last_hash'),
        'last_update': history.get('last_update')
    }

def compare_sheet_versions(spreadsheet_id: str, credentials_path: str, revision_id_1: str, revision_id_2: str) -> dict:
    """
    Сравнивает две версии таблицы
    
    Args:
        spreadsheet_id (str): ID таблицы
        credentials_path (str): Путь к файлу с учетными данными
        revision_id_1 (str): ID первой ревизии
        revision_id_2 (str): ID второй ревизии
        
    Returns:
        dict: Словарь с результатами сравнения
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/drive.readonly'
            ]
        )
        
        drive_service = build('drive', 'v3', credentials=credentials)
        
        # Получаем информацию о ревизиях
        revision_1 = drive_service.revisions().get(
            fileId=spreadsheet_id,
            revisionId=revision_id_1
        ).execute()
        
        revision_2 = drive_service.revisions().get(
            fileId=spreadsheet_id,
            revisionId=revision_id_2
        ).execute()
        
        comparison = {
            'revision_1': {
                'id': revision_1.get('id'),
                'modified_time': revision_1.get('modifiedTime'),
                'size': revision_1.get('size', 0)
            },
            'revision_2': {
                'id': revision_2.get('id'),
                'modified_time': revision_2.get('modifiedTime'),
                'size': revision_2.get('size', 0)
            },
            'size_difference': int(revision_2.get('size', 0)) - int(revision_1.get('size', 0))
        }
        
        return comparison
        
    except Exception as e:
        print(f"Ошибка при сравнении версий: {e}")
        return {} 

def get_detailed_row_changes(old_df: pd.DataFrame, new_df: pd.DataFrame) -> dict:
    """
    Детально сравнивает два DataFrame и находит конкретные изменения
    
    Args:
        old_df (pd.DataFrame): Предыдущее состояние данных
        new_df (pd.DataFrame): Текущее состояние данных
        
    Returns:
        dict: Детальная информация об изменениях
    """
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Начинаем сравнение данных...")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Старые данные: {len(old_df)} строк")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Новые данные: {len(new_df)} строк")
    
    changes = {
        'added_rows': [],
        'deleted_rows': [],
        'modified_rows': [],
        'column_changes': [],
        'summary': {
            'total_changes': 0,
            'rows_added': 0,
            'rows_deleted': 0,
            'rows_modified': 0,
            'cells_changed': 0
        }
    }
    
    # Если DataFrame'ы пустые
    if old_df.empty and new_df.empty:
        return changes
    
    if old_df.empty:
        changes['added_rows'] = new_df.to_dict('records')
        changes['summary']['rows_added'] = len(new_df)
        changes['summary']['total_changes'] = len(new_df)
        return changes
    
    if new_df.empty:
        changes['deleted_rows'] = old_df.to_dict('records')
        changes['summary']['rows_deleted'] = len(old_df)
        changes['summary']['total_changes'] = len(old_df)
        return changes
    
    # Создаем уникальный ключ для каждой строки (комбинация нескольких колонок)
    key_columns = []
    for col in ['Игра', 'Провайдер', 'Старт промо', 'Завершение промо']:
        if col in old_df.columns and col in new_df.columns:
            key_columns.append(col)
    
    if not key_columns:
        # Если нет подходящих колонок, используем индекс
        old_df['_row_key'] = old_df.index
        new_df['_row_key'] = new_df.index
        key_columns = ['_row_key']
    
    # Создаем ключи для строк
    old_df['_composite_key'] = old_df[key_columns].astype(str).agg('||'.join, axis=1)
    new_df['_composite_key'] = new_df[key_columns].astype(str).agg('||'.join, axis=1)
    
    old_keys = set(old_df['_composite_key'])
    new_keys = set(new_df['_composite_key'])
    
    # Найдем добавленные строки
    added_keys = new_keys - old_keys
    for key in added_keys:
        row_data = new_df[new_df['_composite_key'] == key].iloc[0].to_dict()
        del row_data['_composite_key']  # Удаляем служебную колонку
        changes['added_rows'].append({
            'row_key': key,
            'data': row_data
        })
    
    # Найдем удаленные строки
    deleted_keys = old_keys - new_keys
    for key in deleted_keys:
        row_data = old_df[old_df['_composite_key'] == key].iloc[0].to_dict()
        del row_data['_composite_key']  # Удаляем служебную колонку
        changes['deleted_rows'].append({
            'row_key': key,
            'data': row_data
        })
    
    # Найдем измененные строки
    common_keys = old_keys & new_keys
    for key in common_keys:
        old_row = old_df[old_df['_composite_key'] == key].iloc[0]
        new_row = new_df[new_df['_composite_key'] == key].iloc[0]
        
        cell_changes = []
        for column in old_df.columns:
            if column == '_composite_key':
                continue
            
            old_value = old_row[column] if column in old_row else None
            new_value = new_row[column] if column in new_row else None
            
            # Сравниваем значения (учитываем NaN)
            if pd.isna(old_value) and pd.isna(new_value):
                continue
            elif pd.isna(old_value) or pd.isna(new_value) or old_value != new_value:
                cell_changes.append({
                    'column': column,
                    'old_value': str(old_value) if not pd.isna(old_value) else None,
                    'new_value': str(new_value) if not pd.isna(new_value) else None
                })
        
        if cell_changes:
            changes['modified_rows'].append({
                'row_key': key,
                'changes': cell_changes,
                'old_data': {col: str(val) if not pd.isna(val) else None for col, val in old_row.items() if col != '_composite_key'},
                'new_data': {col: str(val) if not pd.isna(val) else None for col, val in new_row.items() if col != '_composite_key'}
            })
    
    # Обновляем сводку
    changes['summary']['rows_added'] = len(changes['added_rows'])
    changes['summary']['rows_deleted'] = len(changes['deleted_rows'])
    changes['summary']['rows_modified'] = len(changes['modified_rows'])
    changes['summary']['cells_changed'] = sum(len(row['changes']) for row in changes['modified_rows'])
    changes['summary']['total_changes'] = (
        changes['summary']['rows_added'] + 
        changes['summary']['rows_deleted'] + 
        changes['summary']['rows_modified']
    )
    
    return changes

def track_detailed_changes(spreadsheet_id: str, range_name: str, credentials_path: str, 
                         tracking_file: str = 'detailed_changes_log.json') -> dict:
    """
    Отслеживает детальные изменения в данных
    
    Args:
        spreadsheet_id (str): ID таблицы
        range_name (str): Диапазон для отслеживания
        credentials_path (str): Путь к файлу с учетными данными
        tracking_file (str): Файл для хранения детальной истории изменений
        
    Returns:
        dict: Детальная информация об изменениях
    """
    from google_sheets import load_sheet_to_df
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Начинаем проверку изменений...")
    
    # Загружаем текущие данные (принудительно, без кэша)
    current_data = load_sheet_to_df(spreadsheet_id, range_name, credentials_path)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Загружено строк: {len(current_data)}, колонок: {len(current_data.columns)}")
    
    # Очищаем данные для стабильного сравнения
    current_data = current_data.replace({None: '', pd.NA: ''}).fillna('')
    
    # Загружаем предыдущее состояние
    if os.path.exists(tracking_file):
        with open(tracking_file, 'r', encoding='utf-8') as f:
            history = json.load(f)
    else:
        history = {'changes': [], 'last_data': None}
    
    # Получаем предыдущие данные
    previous_data = None
    if history.get('last_data'):
        try:
            previous_data = pd.DataFrame(history['last_data'])
        except:
            previous_data = None
    
    # Если это первый запуск
    if previous_data is None:
        change_info = {
            'timestamp': datetime.now().isoformat(),
            'is_first_run': True,
            'has_changes': False,
            'rows_count': len(current_data),
            'columns_count': len(current_data.columns),
            'detailed_changes': None
        }
    else:
        # Сравниваем данные
        detailed_changes = get_detailed_row_changes(previous_data, current_data)
        
        change_info = {
            'timestamp': datetime.now().isoformat(),
            'is_first_run': False,
            'has_changes': detailed_changes['summary']['total_changes'] > 0,
            'rows_count': len(current_data),
            'columns_count': len(current_data.columns),
            'detailed_changes': detailed_changes
        }
    
    # Сохраняем текущие данные для следующего сравнения
    history['last_data'] = current_data.to_dict('records')
    
    # Если есть изменения, добавляем их в историю
    if change_info['has_changes'] or change_info['is_first_run']:
        history['changes'].append(change_info)
        
        # Ограничиваем историю (оставляем последние 50 записей)
        history['changes'] = history['changes'][-50:]
        
        # Сохраняем обновленную историю
        with open(tracking_file, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    
    return change_info

def get_changes_history(tracking_file: str = 'detailed_changes_log.json', limit: int = 10) -> list:
    """
    Получает историю детальных изменений
    
    Args:
        tracking_file (str): Файл с историей изменений
        limit (int): Максимальное количество записей для возврата
        
    Returns:
        list: Список изменений
    """
    if not os.path.exists(tracking_file):
        return []
    
    with open(tracking_file, 'r', encoding='utf-8') as f:
        history = json.load(f)
    
    changes = history.get('changes', [])
    
    # Возвращаем последние записи
    return changes[-limit:] if changes else []

def export_changes_to_excel(changes_data: dict, filename: str = None) -> str:
    """
    Экспортирует детальные изменения в Excel файл
    
    Args:
        changes_data (dict): Данные об изменениях
        filename (str): Имя файла (опционально)
        
    Returns:
        str: Путь к созданному файлу
    """
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'detailed_changes_{timestamp}.xlsx'
    
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        # Сводка изменений
        if changes_data.get('detailed_changes'):
            summary = changes_data['detailed_changes']['summary']
            summary_df = pd.DataFrame([summary])
            summary_df.to_excel(writer, sheet_name='Сводка', index=False)
            
            # Добавленные строки
            if changes_data['detailed_changes']['added_rows']:
                added_df = pd.DataFrame([row['data'] for row in changes_data['detailed_changes']['added_rows']])
                added_df.to_excel(writer, sheet_name='Добавленные строки', index=False)
            
            # Удаленные строки
            if changes_data['detailed_changes']['deleted_rows']:
                deleted_df = pd.DataFrame([row['data'] for row in changes_data['detailed_changes']['deleted_rows']])
                deleted_df.to_excel(writer, sheet_name='Удаленные строки', index=False)
            
            # Измененные строки
            if changes_data['detailed_changes']['modified_rows']:
                modified_data = []
                for row in changes_data['detailed_changes']['modified_rows']:
                    for change in row['changes']:
                        modified_data.append({
                            'Ключ строки': row['row_key'],
                            'Колонка': change['column'],
                            'Старое значение': change['old_value'],
                            'Новое значение': change['new_value']
                        })
                
                if modified_data:
                    modified_df = pd.DataFrame(modified_data)
                    modified_df.to_excel(writer, sheet_name='Измененные ячейки', index=False)
    
    return filename 