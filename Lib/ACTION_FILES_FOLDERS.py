import os
import shutil
import logging
import re
from CONFIG import Config as cfg



class ActionFilesFolders:
    """ Базовый класс для работы с файлами и каталогами """


    def move_copy(self, path, direct, action):
        """
        Перемещение/Копирование файлов
        :param path: путь до файла
        :param direct: путь до директории
        :param action: действие перемещение/копирование
        """
        if os.path.isdir(direct):  # Если direct папка
            file_name = os.path.basename(path)
            direct = os.path.join(direct, file_name)
        if self.path_exists(direct):  # Удаляем файл, если существует (для перезапусков)
            os.remove(direct)
        if action == 'move':
            logging.info(f'Перемещаем "{os.path.basename(path)}" из каталога: '
                         f'"{os.path.dirname(path)}" в каталог: "{os.path.dirname(direct)}"')
            shutil.move(path, direct)
        if action == 'copy':
            logging.info(f'Копируем "{os.path.basename(path)}" из каталога: '
                         f'"{os.path.dirname(path)}" в каталог: "{os.path.dirname(direct)}"')
            shutil.copy2(path, direct)

    def create_folder(self, folder):
        """
        Создание новой директории
        :param folder: путь до директории
        """
        if not self.path_exists(folder):
            logging.info('Создаю каталог: {}'.format(folder))
            os.mkdir(folder)

    @staticmethod
    def path_exists(path):
        """
        Проверка существования директории или файла
        :param path: путь
        :return: True
        """
        logging.info('Проверяем путь: ' + path)
        try:
            if os.path.isdir(path): file_or_folder = 'Каталог'
            else: file_or_folder = 'Файл'
        except TypeError:
            file_or_folder = 'Указанная ссылка'
        if os.path.exists(path):
            logging.info(f'{file_or_folder} существует')
            return True
        else:
            logging.info(f'{file_or_folder} не существует')

