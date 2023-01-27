import logging
import pika
import json
from business import Business
from CONFIG import Config as cfg
from Lib import EXCEPTION_HANDLER
from Lib import log
from pika.exceptions import AMQPConnectionError
from datetime import datetime
import time


class Robot(Business):

    """
    Название: Получение информации по индексации цен из МЭР
    Аналитик:
    Разработчик: Цыганов С.А.
    """

    def __init__(self):
        super().__init__()

    def queue_processing(self):
        """
        Проверка очереди в бесконечном цикле
        """
        credentials = pika.PlainCredentials(cfg.LOGIN, cfg.PWD)
        parameters = pika.ConnectionParameters(cfg.HOST, cfg.PORT, cfg.PATH, credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        logging.info('Подключение к RabbitMQ прошло успешно')

        def callback(ch, method, properties, body):
            logging.info(f'Получен запрос')
            self.queue_response = None
            self.type_request = None
            self.task_id = None
            self.forecast = None
            self.not_found_files = None
            # не давать нов задачу пока не сделает имеющуюся
            ch.basic_qos(prefetch_count=1)
            # Подтверждение получения сообщения. Без него сообщения будут выводиться заново после падения обработчика.
            ch.basic_ack(delivery_tag=method.delivery_tag)
            if self.request_validator(body) is True:
                self.data_process()
                logging.info('Жду запрос')

        try:
            logging.info('Жду запрос')
            channel.basic_consume(queue=cfg.queue_request, on_message_callback=callback)
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
            logging.info("Обработка очереди завершена")
        channel.close()
        logging.info("Соединение закрыто")

    @EXCEPTION_HANDLER.exception_decorator
    def request_validator(self, body):
        """
        Проверка влидности запроса
        :param data: десериализованное тело запроса
        :return: True or False
        """
        data = json.loads(body)
        logging.info(data)
        errors = ''
        if data['header']['replayRoutingKey'] == '':
            errors = 'Поле replayRoutingKey в запросе пустое. '
        else:
            self.queue_response = data['header']['replayRoutingKey']

        if data['header']['subject'] == '':
            errors = 'Поле subject в запросе пустое. '
        else:
            if data['header']['subject'] != 'IndexMED':
                errors = f"{errors}Несоотвествующий тип запроса. Получили {data['header']['subject']} ожидалось IndexMED "
            else:
                self.type_request = data['header']['subject']

        if data['header']["requestID"] == '':
            errors = 'Поле requestID в запросе пустое. '
        else:
            self.task_id = data['header']["requestID"]

        if data['body']['forecast'] == '':
            errors = 'Поле forecast в запросе пустое. '
        else:
            if data['body']['forecast'] == 'medium-term' or data['body']['forecast'] == 'medium-condition' or \
                    data['body']['forecast'] == 'long-term':
                self.forecast = data['body']['forecast'] # long-term долгосрочный прогноз, medium-term среднесрочный прогноз, medium-condition сценарные условия
            else:
                errors = f"{errors}Прогноз должен быть long-term или medium-term или medium-condition. Проверьте пробелы. Получили {data['body']['forecast']}"

        if errors:
            logging.error(errors)
            queue = data['header']['replayRoutingKey']
            if data['header']['replayRoutingKey'] == '':
                queue = cfg.queue_error
            EXCEPTION_HANDLER.ExceptionHandler().exception_handler(queue=queue,
                                                                   text_error=errors,
                                                                   task_id=data['header']["requestID"],
                                                                   type_error='bad_request',
                                                                   to_rabbit='on',
                                                                   to_mail='on'
                                                                   )
            return False
        logging.info(f'Данные валидны')
        return True

    @EXCEPTION_HANDLER.exception_decorator
    def run(self,  max_tries=5):
        """
        Запуск робота
        :param max_tries: число попыток подкдючения к RabbitMQ
        """
        start_time = ''
        while True:
                try:
                    self.queue_processing()
                except AMQPConnectionError:
                    logging.error('Ошибка подключения к серверу RabbitMQ')
                    if max_tries == 0:
                        time_errors = datetime.now().minute - start_time.minute
                        #Если ошибки подключения продолжаются в течении 5 минут
                        if time_errors <= 5:
                            EXCEPTION_HANDLER.ExceptionHandler().exception_handler(
                                type_error='connect_rabbit_error',
                                to_mail='on',
                                stop_robot='on'
                            )
                        else:
                            max_tries = 5
                            continue
                    else:
                        #Отсчет времени начала ошибок
                        start_time = datetime.now()
                        max_tries -= 1
                        logging.error('Пробую повторно подключиться к серверу RabbitMQ через 1мин')
                        time.sleep(60)
                        continue
                except KeyboardInterrupt:
                    logging.error('Робот отсановлен')
                    break



if __name__ == '__main__':
    log.set_2(cfg)
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.info('\n\n=== Start ===\n\n')
    logging.info(f'Режим запуска: {cfg.mode}')
    Robot().run()