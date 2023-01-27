"""Импорты"""
import logging
import os
import logging as lg
import re
import time
from zipfile import ZipFile
import shutil
from Lib.EXCEPTION_HANDLER import ExctractError
from CONFIG import Config as cfg
from pathlib import Path
import zipfile
from Lib.EXCEPTION_HANDLER import FileProcessError
from datetime import datetime



class Zipworker:
    """Класс по работе со скаченным архивом"""

    def file_compression(self, folder_load, processed_folder):
        """
        Архивайия загруженных файлов
        :param folder_load: директория загрузки файлов
        :param processed_folder: директория хранения обработанных файлов
        :return:
        """
        self.processed_folder_compression(os.path.join(folder_load, 'Processed_files'))
        os.chdir(folder_load)
        os.mkdir(processed_folder)
        for file in os.listdir(folder_load):
            if 'Processed_files' not in file and 'Screen' not in file:
                if '.xlsx' in file:
                    file_name = file.replace('.xlsx', '')
                elif '.json' in file:
                    file_name = file.replace('.json', '')
                else:
                    raise FileProcessError('Ошибка обработки файла. Проверьте расширение файлов')
                path_to_zip = os.path.join(folder_load, f'{file_name}.zip')
                with zipfile.ZipFile(path_to_zip, 'w') as zip:
                    zip.write(file, compress_type=zipfile.ZIP_DEFLATED)
                logging.info(f'Архив {file_name}.zip создан')
                logging.info(f'Перемещаем "{file}" из каталога: '
                             f"{Path(folder_load, file)} в каталог: {processed_folder}")
                shutil.move(os.path.join(folder_load, file), processed_folder)
        os.chdir(cfg.folder_root)

    def processed_folder_compression(self, processed_files):
        """
        Архивация фалов в директории Processed_files, если суммарный объем превышает cfg.size
        :param folder_load:
        :return:
        """

        current_size = 0
        list_dir = []
        for dir in os.listdir(processed_files):
            if 'zip' in dir:
                continue
            else:
                dir_path = os.path.join(processed_files, dir)
                list_dir.append(dir)
                for file in os.listdir(dir_path):
                    filename = os.path.join(dir_path, file)
                    current_size += os.path.getsize(filename) / 1024 / 1024

        os.chdir(processed_files)
        if current_size >= cfg.size:
            path_to_zip = os.path.join(f'Архив_от_{datetime.today().strftime("%d.%m.%Y %H-%M-%S")}.zip')
            with zipfile.ZipFile(path_to_zip, 'w') as zip:
                for dir in list_dir:
                    for file in os.listdir(dir):
                        file_name = os.path.join(dir, file)
                        zip.write(file_name, compress_type=zipfile.ZIP_DEFLATED)
                        os.remove(file_name)
                    os.rmdir(dir)
            logging.info(f'Архивация файлов произведена. Создан архив {path_to_zip}')
        os.chdir(cfg.folder_root)


    def remove_zip(self, name_zip, match):
        """
        Удаляет скаченный архив, если все файлы найдены
        :param name_zip: имя архива
        :param match: список имен не найденных фалов в архиве
        """
        if len(match) == 0:
            os.remove(name_zip)
        elif 2 >= len(match) >= 1:
            logging.error(f'Не найдены файлы в архиве: {list(match.keys())}')
            os.remove(name_zip)

    @staticmethod
    def return_zipname(load_dir, max_tries=5):
        """
        Возвращает имя скаченного архива
        :param load_dir: директория со скаченным архивом
        :param max_tries: число попыток найти скаченный архив
        :return: путь до скаченного архива
        """
        while max_tries >= 0:
            for name in os.listdir(load_dir):
                if re.findall('.zip$', name):
                    time.sleep(2)
                    name_zip = os.path.join(load_dir, name)
                    logging.info(f'Найден архив {name_zip}')
                    return name_zip
            else:
                max_tries -= 1
                time.sleep(10)
                logging.info(f'Ищу скаченный архив. Повторная попытка. Осталось {max_tries}')
        logging.error('Не найден скаченный архив')
        raise


    def unpack_zipfile(self, exctract_dir, zip_templates):
        """
        Распаковываем архив
        :param exctract_dir: директория для извлечения файлов из архива
        :param zip_templates: шаблон имен файлов для поиска их в архиве
        """
        try:
            name_zip = self.return_zipname(exctract_dir)
            lg.info(f"Начинаю работу с архивом {name_zip}")
            with ZipFile(name_zip) as archive:
                # Перебирем файлы в архиве
                for entry in archive.infolist():
                    name = entry.filename.encode('cp437').decode('cp866')
                    #Перебираем шаблон имен файлов
                    for name_file, template in zip_templates.items():
                        # Если имя совпадает с шаблоном, то извлекаем файл
                        if re.fullmatch(template, name):
                            target = Path(exctract_dir, f'{name_file}.xlsx')
                            logging.info(f'Найден файл {name_file}')
                            with archive.open(entry) as source, open(target, 'wb') as dest:
                                shutil.copyfileobj(source, dest)
                                #Удаляем из шаблона найденный файл
                                zip_templates.pop(name_file)
                                logging.info(f'Файл {name_file} извлечен')
                                break
            #Удаляем архив после извлечения файлов
            self.remove_zip(name_zip, zip_templates)
            lg.info(f"Все файлы извлечены")
            #Если есть не найденные файлы, их количество меньше трех, то возвращаем их имена
            if zip_templates:
                list_not_found_files = [file for file in zip_templates.keys()]
                return list_not_found_files
            return zip_templates
        except Exception as err:
            raise ExctractError(f'Ошибка при извлечении файла из архива {exctract_dir}')


if __name__ == '__main__':
    zip = Zipworker()
    zip.return_zipname(r'Load/test/долгосрочный прогноз')