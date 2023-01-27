"""Импорты"""
import os
from datetime import datetime

MODE = 'test'



class Config:
    """Конфигурационный класс"""
    mode = MODE

    #Общие параметры
    robot_name = 'Робот: Получение информации по индексации цен из МЭР'
    theme_mail = 'Выгрузка индексов цен'
    folder_root = os.path.dirname(os.path.abspath(__file__))
    folder_load = os.path.join(folder_root, os.path.join('Load', MODE))
    logs_folder_path = os.path.join(folder_root, os.path.join('Logs', MODE))
    log_file = "robot.log"
    folder_templates = os.path.join(folder_root, 'Templates')
    log_tasks = os.path.join(logs_folder_path, 'log.xlsx')
    log_robot = os.path.join(logs_folder_path, 'Log_info.log')
    response = os.path.join(folder_templates, 'response.json')
    file_data = os.path.join(folder_templates, 'file_data.json')
    send_error_dir = os.path.join(folder_root, 'Send_error')
    robot_mail = ''
    server_mail = "smtp.rosatom.ru"
    # Общий параметры xpath для всех прогнозов.
    url = r'https://economy.gov.ru/material/directions/makroec/prognozy_socialno_ekonomicheskogo_razvitiya/'

    # Выбор типа параметров
    if mode.lower() == 'prod':
        support_email = ''
        browser_path = r''
        driver_path = r''
        load_path = r''
        current_year = ''
        work_date = []
        work_month = []
        log_limit = 50
    elif mode.lower() == 'test':
        #размер фалов для архивации
        size = 1000
        support_email = ''
        chrome_path = r''
        chrome_driver = r''
        # Настройки rabbit для ''
        HOST = ''
        LOGIN = ''
        PWD = os.environ['rpauser']
        queue_request = ''
        PORT = 5672  # Порт для подключения к серверу с rabbit
        PATH = '/'
        queue_error = ''

    # Директории для хранения загруженных с сайта файлов
    folder_load_forecast = {
        'medium-condition': os.path.join(folder_load, 'сценарные условия'),
        'medium-term': os.path.join(folder_load, 'среднесрочный прогноз'),
        'long-term': os.path.join(folder_load, 'долгосрочный прогноз')
    }
    #Директории для хранения переданных в шину данных
    processed_files_medium_condition = os.path.join(folder_load_forecast['medium-condition'], 'Processed_files')
    processed_files_medium_term = os.path.join(folder_load_forecast['medium-term'], 'Processed_files')
    processed_files_long_term = os.path.join(folder_load_forecast['long-term'], 'Processed_files')

    #Директории для хранения скриншотов
    folder_screen_forecast = {
        'medium-condition': os.path.join(folder_load_forecast['medium-condition'], 'Screen'),
        'medium-term': os.path.join(folder_load_forecast['medium-term'], 'Screen'),
        'long-term': os.path.join(folder_load_forecast['long-term'], 'Screen')
    }


    # Создаем директории
    [os.makedirs(dir, exist_ok=True) for dir in
     [
         folder_load_forecast['medium-condition'],
         folder_load_forecast['medium-term'],
         folder_load_forecast['long-term'],
         folder_screen_forecast['medium-condition'],
         folder_screen_forecast['medium-term'],
         folder_screen_forecast['long-term'],
         processed_files_medium_condition,
         processed_files_medium_term,
         processed_files_long_term,
         logs_folder_path,
         folder_templates,
         send_error_dir
     ]]

