"""Импорты"""
import logging
import os
from business_website import WebsiteWorker
from CONFIG import Config as cfg
from zip_worker import Zipworker
from Lib.EXCEL import Excel
from Lib.RABBIT import Rabbit
from Lib import EXCEPTION_HANDLER
from Lib.ACTION_FILES_FOLDERS import ActionFilesFolders
from pathlib import Path
from datetime import datetime
import shutil
import json
import base64


class Business:
    """Класс реализации главной логики"""

    def __init__(self):
        self.action = ActionFilesFolders()
        self.zip = Zipworker()  # объект для работы с архивом
        self.excel = Excel()
        self.rabbit = Rabbit()
        self.queue_response = None
        self.task_id = None
        self.type_request = None
        self.forecast = None
        self.not_found_files = None

    @EXCEPTION_HANDLER.exception_decorator
    def data_process(self):
        """
        В этом блоке реализуется главная логика загрузки архивов с сайта и извлечению файлов
        """
        logging.info('Робот начал обработку сайта')
        # Перебираем Прогнозы
        folder_load = cfg.folder_load_forecast[self.forecast]
        screen_path = cfg.folder_screen_forecast[self.forecast]

        if self.forecast == 'long-term':
            zip_templates = {
                'Дефлятор базовый': r'.{0,}Дефлятор.{0,}баз.{0,}',
                'Экспорт-импорт базовый': r'.{0,}Экспорт-импорт баз.{0,}',
                'ИПЦ базовый': r'.{0,}ИПЦ.{0,}баз.{0,}'
            }
        else:
            zip_templates = {
                'Дефлятор базовый': r'.{0,}Дефлятор.{0,}баз.{0,}',
                'Внешняя торговля базовый': r'.{0,}Внеш.{0,}торг.{0,}баз.{0,}',
                'ИПЦ базовый': r'.{0,}ИПЦ.{0,}баз.{0,}'
            }
        self.clean_folder(folder_load)
        # Если прогнозы найдены на сайте
        webworker = WebsiteWorker(folder_load=folder_load)
        if webworker.work_with_site(
                                  url=cfg.url,
                                  forecast=self.forecast,
                                  screen_path=screen_path,
                                  task_id=self.task_id,
                                  queue_response=self.queue_response):

            # Извлекаем файлы из скаченного архива
            self.not_found_files = self.zip.unpack_zipfile(folder_load, zip_templates)
            if len(self.not_found_files) == 3:
                raise EXCEPTION_HANDLER.NotFoundFiles('В архиве не найдено ни одного файла')
            # Обрабатываем извлеченные данные и создаем file_data.json
            self.excel.file_processing(folder_load, self.forecast, self.task_id, webworker.date_update)
            # создаем подпапку для хранения обработанных фалов
            processed_folder = Path(folder_load, 'Processed_files',
                                    f'Загруженные данные {datetime.today().strftime("%d.%m.%Y %H-%M-%S")}')
            logging.info(f'Создана директория {processed_folder}')
            # создаем архив из извлеченных данных, возвращаем список имен ненайденных фалов
            self.zip.file_compression(folder_load, processed_folder)
            # готовим json для отправки в шину
            data_for_send = self.create_response(folder_load, webworker.date_update)
            self.rabbit.send_data_queue(self.queue_response, data_for_send)
            # перемещаем отправленный json в processed_folder
            self.excel.add_data_to_log(self.task_id, self.forecast, 'Выполнено')
            shutil.move(data_for_send, processed_folder)
            # Если найден долгосрочный прогноз, перезаписываем год в файле
            if self.forecast == 'long-term':
                with open('Templates/year.txt', 'r+') as file:
                    text = file.readline()
                    year = int(text) + 18
                    file.seek(0)
                    file.write(str(year))
                    logging.info(f'Значение года в файле year.txt перезаписано. Стало {year}')
            # self.wb.close_site()
        # Если данные не найдены
        else:
            logging.info(f"Прогноз {self.forecast} не найден на сайте.")
            EXCEPTION_HANDLER.ExceptionHandler().exception_handler(queue=self.queue_response,
                                                                   task_id=self.task_id,
                                                                   forecast=self.forecast,
                                                                   type_error='no_updates',
                                                                   to_rabbit='on'
                                                                   )
        webworker.close_site()

    def create_response(self, folder_load, date_update):
        logging.info(f'Начинаю запись данных в response.json')
        with open(cfg.response, mode='r', encoding='utf-8') as file:
            data = json.load(file)
            data["header"]["timestamp"] = datetime.timestamp(datetime.now())
            data["header"]["requestID"] = self.task_id
            data["body"]["forecast"] = self.forecast
            data["body"]["date_update"] = date_update
            if self.not_found_files:
                data["not_found_files"] = self.not_found_files

        for file in os.listdir(folder_load):
            if 'zip' in file:
                file_path = os.path.join(folder_load, file)
                if 'file_data' in file:
                    data["body"]["file_data"] = self.convert_to_base64(file_path)
                else:
                    data["body"]["files"].append({"name": file, "base64": self.convert_to_base64(file_path)})
                os.remove(os.path.join(folder_load, file))

        data_for_send = os.path.join(folder_load, 'response.json')
        json.dump(data, open(data_for_send, mode='w',
                             encoding='utf-8'), indent=4, ensure_ascii=False, default=str)
        logging.info(f'Запись данных в response.json закончена')
        return data_for_send

    def convert_to_base64(self, file_path):
        with open(file_path, 'rb') as f:
            doc64 = base64.b64encode(f.read())
            logging.info(f'Закодировал {file_path} в base64')
            doc_str = doc64.decode('utf-8')
            return doc_str

    def clean_folder(self, folder_load):
        """Очищаем Load от файлов"""
        for file in os.listdir(folder_load):
            if os.path.isfile(os.path.join(folder_load, file)):
                os.remove(os.path.join(folder_load, file))
                logging.info(f'{file} удален')
