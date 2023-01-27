import logging
from Lib.SELENIUM import Selenium
from CONFIG import Config as cfg
import os
from datetime import datetime
import re
from Lib import EXCEPTION_HANDLER


class WebsiteWorker(Selenium):
    """Класс для построения бизнес процесса при работе с сайтом"""
    def __init__(self, folder_load):
        super().__init__(folder_load)
        self.current_year = None
        self.date_update = None

    def work_with_site(self, url,
                       forecast, screen_path, task_id, queue_response):
        """
        Поиск прогноза на сайте
        :param folder_load: директория загрузки файла с сайта
        :param url: адрес сайта
        :param forecast: прогноз
        :param screen_path: путь до директории со скриншотами
        :return: True - прогноз найден or False - прогноз не найден
        """

        self.current_year = 2018 #2021 для среднесрочного, 2018 для долгосрочного,  для текущей даты datetime.now().year
        xpath = self.return_xpath(forecast)
        logging.info(f'Ищим прогноз {xpath["link"]}')
        count = 1
        # В цикле проходим по страницам сайта ищем элемент
        while True:
            full_url = f'{url}?page={count}'
            self.open_site(task_id, queue_response, site_url=full_url)
            # Если страница содержит нужный блок
            if self.exists_by_xpath(xpath['div']):
                # Если страница содержит нужный прогноз
                forecast_found = self.search_forecast(xpath)
                if forecast_found:
                    self.find_by_xpath(forecast_found).click()
                    self.get_screen_shot(screen_path)
                    self.download_file(xpath["downfile"])
                    return True
                # Если Прогноз на странице не найден, то переходим на следующую
                else:
                    count += 1
                    logging.info('Ищем дальше')
                    continue
            else:
                # Если на cтранице нет блока div прекращаем поиск
                return False

    def return_xpath(self, forecast):
        """
        По имени Прогноза возвращает xpath для данного прогноза
        :param forecast: прогноз
        :return: возвращает словарь с xpath
        """
        exctract_year = self.exctract_year()
        xpath = {'downfile': '//span[text()="Приложения"]',
                 'div': '//div[@id="submaterails"]'}
        # индивидуальный xpath для каждого прогноза
        xpath_link = {'medium-condition': [r'//a[contains(@title,"Сценарные условия")]',
                                           r'//a[contains(@title,"Основные параметры сценарных условий")]'],
                      'medium-term': [
                          fr'//a[contains(@title, "Прогноз социально-экономического развития Российской Федерации на {self.current_year + 1} год и на плановый период {self.current_year + 2} и {self.current_year + 3} годов")]'],
                      'long-term': [
                          fr'//a[contains(@title, "Прогноз социально-экономического развития Российской Федерации на период до {exctract_year} года")]']}
        xpath['link'] = xpath_link.get(forecast)
        return xpath

    def exctract_year(self):
        """
        Функция извлечения значения года из файла year.txt
        :return: возвращает значение года из файла year.txt
        """
        with open('Templates/year.txt') as file:
            text = file.readline()
            year = int(text) + 18
            return year

    def str_to_date(self, text):
        """
        Перевод значения типа строки в дату
        :param text: Формат даты "28 Сентября 2022", извлеченной с сайта
        :return: формат даты
        """
        index_month = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5,
            'июня': 6, 'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10,
            'ноября': 11, 'декабря': 12
        }
        if re.fullmatch(r'\d+\s[a-яА-Я]+\s\d\d\d\d$', text):
            month_text = re.search(r'[a-яА-Я]+', text)[0]
            month = index_month[month_text.lower()]
            day = int(re.search(r'^\d+', text)[0])
            year = int(re.search(r'\d\d\d\d$', text)[0])
            date = datetime(year, month, day)
            return date
        else:
            logging.error('Возможно, изменился формат даты обновления прогноза на сайте')
            raise EXCEPTION_HANDLER.NotFoundElement('Возможно, изменился формат даты обновления прогноза на сайте')

    def search_forecast(self, xpath):
        """
        Поиск xpath с прогнозам на сайте
        :param xpath: словарь с xpath
        :return: webelement or False
        """
        for element in xpath['link']:
            if self.exists_by_xpath(element):
                name_forecast = self.find_by_xpath(element)
                self.date_update = self.str_to_date(self.find_by_xpath(fr'{element}//small[@class="e-date"]').text)
                if self.current_year == self.date_update.year:
                    logging.info(f'Найден прогноз от {name_forecast.text}')
                    return element
        return False
