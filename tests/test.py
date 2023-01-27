import json
import unittest
from CONFIG import Config as cfg
from producer import Rabbit
import os


class Test:
    def __init__(self):
        self.producer = Rabbit()
        self.medium_term = {'file_data.json': '', 'response.json': '', 'Дефлятор базовый.xlsx': '',
                       'ИПЦ базовый.xlsx': '', 'Внешняя торговля базовый.xlsx': ''}
        self.long_term = {'file_data.json': '', 'response.json': '', 'Дефлятор базовый.xlsx': '',
                     'ИПЦ базовый.xlsx': '', 'Экспорт-импорт базовый.xlsx': ''}

    def run_test(self):
        self.test_forecast(self.medium_term, 'medium_term', cfg.processed_files_medium_term)
        self.test_forecast(self.medium_term, 'medium_condition', cfg.processed_files_medium_condition)
        self.test_forecast(self.long_term, 'long_term', cfg.processed_files_long_term)

    def send_message(self):
        # right_json = os.path.join(cfg.folder_root, 'tests', 'example_requests', 'medium_term.json')
        # self.producer.producer_queue(queue_name=cfg.queue_request, data_path=right_json)
        #
        # right_json = os.path.join(cfg.folder_root, 'tests', 'example_requests', 'medium_condition.json')
        # self.producer.producer_queue(queue_name=cfg.queue_request, data_path=right_json)

        right_json = os.path.join(cfg.folder_root, 'tests', 'example_requests', 'long_term.json')
        self.producer.producer_queue(queue_name=cfg.queue_request, data_path=right_json)

    def test_forecast(self, temlates, forecast, processed_files):
        """
        Очистить все папки Processed_files, выполнить нужный запрос send_message(), запустить test_forecast()
        Проверка наличия файлов в папке processed_files после обработки запроса
        :return:
        """
        uploaded_file = False
        response_exist = False
        header_body = False
        for dir in os.listdir(processed_files):
            for file in os.listdir(os.path.join(processed_files, dir)):
                #Проверка скаченных файлов.
                assert (file in temlates)
                uploaded_file = True
                # Проверка заполненности всех полей в файле response
                if 'response' in file:
                    response_exist = True
                    with open(os.path.join(os.path.join(processed_files, dir), file), 'r',
                              encoding='utf-8') as response:
                        data = json.load(response)
                    for tag in data:
                        if tag == 'header' or tag == 'body':
                            header_body = True
                            for key in data[tag]:
                                try:
                                    assert (data[tag][key] != '')
                                except AssertionError as err:
                                    header_body = False
                                    print(f'Тег "{key}" не заполнен в {forecast}: {err}')

            if uploaded_file is True and response_exist is True and header_body is True:
                print(f'Test {forecast} PASSED')
            else:
                print(f'Test {forecast} ERROR')






if __name__ == '__main__':
    # Test().run_test()
    Test().send_message()
