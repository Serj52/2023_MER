"""Импорты"""
import os
import logging as lg
from datetime import datetime
import time

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from CONFIG import Config
import Lib.EXCEPTION_HANDLER


class Connection:
    """Класс по настройке соединения"""

    def __init__(self, folder_load):
        self.config = Config()
        self.options = webdriver.ChromeOptions()
        self.browser_path = self.config.chrome_path
        self.driver_path = self.config.chrome_driver
        self.folder_load = folder_load

    def set_options(self):
        """
        Установка опций вебдрайвера
        :param folder_load: директория загрузки файла
        :return:
        """
        self.options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.options.binary_location = self.browser_path
        self.options.add_experimental_option('prefs', {'download.default_directory': self.folder_load,
                                                       "safebrowsing.enabled": False,
                                                       "download.prompt_for_download": False,
                                                       "download.directory_upgrade": True,
                                                       })
        # Без графического интерфейса браузера
        # self.options.add_argument("--headless")

    def set_driver(self):
        """Создание вебдрайвера"""
        self.driver = webdriver.Chrome(options=self.options, executable_path=self.driver_path)


class Selenium:
    """Класс по работе с Selenium"""
    def __init__(self, folder_load):
        self.conn = Connection(folder_load)
        self.config = Config()
        self.conn.set_options()
        self.conn.set_driver()

    def open_site(self, task_id, queue_response, site_url, max_tries=4):
        """
        Открытие сайта
        :param folder_load: директория загрузки файла
        :param site_url: электронный адрес сайта
        :param max_tries: число попыток открыть сайт
        :return:
        """
        while max_tries > 0:
            max_tries -= 1
            try:
                lg.info(f'Open URL:{site_url}')
                self.conn.driver.get(site_url)
                break
            except Exception as err:
                lg.error(f'Сайт не доступен {err}')
                if max_tries == 0:
                    lg.error(f'Количество попыток подключиться к сайту исчерпано')
                    raise Lib.EXCEPTION_HANDLER.WebsiteError('Ошибка при открытии сайта')
                elif max_tries == 1:
                    lg.info(f'Попробую еще раз открыть сайт через 120 мин')
                    Lib.EXCEPTION_HANDLER.ExceptionHandler().exception_handler(queue=queue_response,
                                                                           task_id=task_id,
                                                                           type_error='robot_sleep',
                                                                           to_rabbit='on',
                                                                           )
                    time.sleep(7200)
                else:
                    lg.info(f'Попробую еще раз открыть сайт через 5сек')
                    time.sleep(5)

    def close_site(self):
        """Закрытие сайта"""
        try:
            self.conn.driver.implicitly_wait(2)
            lg.info("Идет завершение сессии...")
            self.conn.driver.quit()
            lg.info("Драйвер успешно завершил работу.")
        except Exception as error:
            lg.exception(error, exc_info=True)
            os.system("pkill chromium")
            lg.info('Chrome браузер закрыт принудительно.')
            os.system("pkill chromedriver")
            lg.info('Chrome драйвер закрыт принудительно.')

    def find_by_xpath(self, selector, timeout=None):
        """
        Возвращает элемент по xpath
        :param selector: селектор в виде строки
        :param timeout: время(секунды) ожидания элемента на странице
        :return: webelement
        """
        try:
            if timeout:
                wt = WebDriverWait(self.conn.driver, timeout=timeout)
                # Ожидание загрузки тела страницы
                wt.until(EC.element_to_be_clickable((By.XPATH, selector)))
                # return self.conn.driver.find_element(By.XPATH, selector)
            element = self.conn.driver.find_element(By.XPATH, selector)
            actions = ActionChains(self.conn.driver)
            actions.move_to_element(element).perform()
            return element
        except Exception as err:
            raise Lib.EXCEPTION_HANDLER.NotFoundElement(f'Не найден селектор {selector}. Ошибка {err}')

    def exists_by_xpath(self, selector):
        """
        Проверяет наличие элемента на странице
        :param selector: селектор в виде строки
        :return: True or False
        """
        try:
            self.conn.driver.find_element(By.XPATH, selector)
            return True
        except NoSuchElementException:
            return False
        except Exception as err:
            raise Lib.EXCEPTION_HANDLER.NotFoundElement(f'Не найден селектор {selector}. Ошибка {err}')

    def download_file(self, selector, max_tries=3):
        """
        в этом блоке скачивается файл
        :param selector: селектор в виде строки
        :param max_tries: число попыток скачать файл
        :return:
        """
        while True:
            try:
                lg.info('Начинаю загрузку данных')
                self.conn.driver.find_element(By.XPATH, selector).click()
                # Ожидание завершения загрузки 5с
                time.sleep(5)
                lg.info('Данные загружены')
                break
            except Exception as err:
                lg.error(f'Данные не загружены {err}')
                if max_tries == 0:
                    lg.error(f'Количество попыток загрузить данные исчерпано')
                    raise Lib.EXCEPTION_HANDLER.DownloadError('Не удалось загрузить файл с сайта')
                lg.info(f'Пробуем еще раз загрузить файл')
                time.sleep(2)
                max_tries -= 1

    def get_screen_shot(self, screen_path):
        """
        Сделать скриншот
        :param screen_path: директория хранения скриншота
        :return:
        """
        time.sleep(5)
        date = datetime.now().strftime("%d.%m.%Y %H-%M")
        name = f'{date}.png'
        self.conn.driver.save_screenshot(os.path.join(screen_path, name))

if __name__ == '__main__':
    s = Selenium()
    s.open_site(folder_load=Config.folder_load,
                site_url=r'https://economy.gov.ru/material/directions/makroec/prognozy_socialno_ekonomicheskogo_razvitiya/')
