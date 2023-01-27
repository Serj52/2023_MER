import pika
import json
import time
from CONFIG import Config as cfg
import os
import sys
import logging
from datetime import datetime
from pika.exceptions import AMQPConnectionError
import Lib.EXCEPTION_HANDLER


class Rabbit:
    """Класс для работы с очередями на случай использования в проект брокера сообщений"""

    def create_connection(self, max_tries = 5):
       while True:
            try:
                max_tries -= 1
                credentials = pika.PlainCredentials(cfg.LOGIN, cfg.PWD)
                parameters = pika.ConnectionParameters(cfg.HOST, cfg.PORT, cfg.PATH, credentials)
                connection = pika.BlockingConnection(parameters)
                return connection
            except AMQPConnectionError as err:
                if max_tries == 0:
                    Lib.EXCEPTION_HANDLER.ExceptionHandler().exception_handler(
                        type_error='connect_rabbit_error',
                        to_mail='on'
                    )
                    logging.error('Попытки подключиться к серверу RabbitMQ исчерпаны. Данные в очередь не переданы')
                    raise
                else:
                    logging.error(f'Ошибка подключения к серверу RabbitMQ при отправки сообщения.\n'
                                  f'Пробую повторно подключиться через 1мин')
                    time.sleep(60)
                    continue
            except Exception as err:
                logging.error(f'Ошибка подключения к серверу RabbitMQ {err}. Данные в очередь не переданы')
                Lib.EXCEPTION_HANDLER.ExceptionHandler().exception_handler(
                    type_error='connect_rabbit_error',
                    to_mail='on'
                )
                raise

    def send_data_queue(self, queue_response, data, type=None):
        channel = self.create_connection().channel()
        with open(data, 'rb') as file:
            messageBody = file.read()
            logging.info('Отправляю файл с данными в очередь')
        # Отметить сообщения как устойчивые delivery_mode=2, защищенные от потери
        channel.basic_publish(exchange='',
                              routing_key=queue_response,
                              body=messageBody,
                              properties=pika.BasicProperties(delivery_mode=2, )
                              )
        logging.info(f'Сообщение отправлено в очередь {queue_response}')
        path_file = os.path.join(cfg.send_error_dir, f'forecast_{datetime.today().strftime("%d.%m.%Y %H-%M")}.json')
        #сохранение отправленного json в папке с запросом
        if type is None:
            json.dump(json.loads(messageBody), open(path_file, mode='w',
                                        encoding='utf-8'), indent=4, ensure_ascii=False)
        self.create_connection().close()

    def consumer_queue(self):
        """Метод приема сообщений через очередь"""
        channel = self.create_connection().channel()
        # Создается очередь.устойчивая очередь к падению сервера с rabbit mq
        channel.queue_declare(queue=cfg.queue_request, durable=True)

        def callback(ch, method, properties, body):
            time.sleep(3)
            data = json.loads(body)
            exctracted_queue = data
            logging.info(f'Получен запрос из очереди')
            # не давать нов задачу пока не сделает имеющуюся
            ch.basic_qos(prefetch_count=1)
            # Подтверждение получения сообщения. Без него сообщения будут выводиться заново после падения обработчика.
            ch.basic_ack(delivery_tag=method.delivery_tag)

        # on_message_callback=callback даже если вы убьете рабочего с помощью CTRL+C во время обработки сообщения, ничего не будет потеряно.
        # Вскоре после смерти работника все неподтвержденные сообщения будут доставлены повторно.

        channel.basic_consume(queue=cfg.queue_request, on_message_callback=callback)
        logging.info(f'Жду сообщения')
        print(f'Жду сообщения')
        channel.start_consuming()

    def producer_exchange(self, data_path):
        """Метод передачи потребителям через exchange. Сообщения получат все"""
        channel = self.create_connection().channel()
        channel.exchange_declare(exchange=cfg.exchange_responce, exchange_type='direct')
        with open(data_path) as file:
            messageBody = file.read()
            channel.basic_publish(exchange=cfg.exchange_responce, routing_key='hello',
                                  body=messageBody,
                                  properties=pika.BasicProperties(
                                      expiration='60000'))
        print("Sent")
        time.sleep(2)
        self.create_connection().close()

    def consumer_exchange(self, exchange):
        """Метод приема сообщений через exchange"""
        channel = self.create_connection().channel()
        channel.exchange_declare(exchange=exchange, exchange_type='fanout')
        result = channel.queue_declare(queue='', exclusive=True, durable=True)  # exclusive=True уникальная очередь, будет удалена
        queue_name = result.method.queue
        channel.queue_bind(exchange=exchange, queue=queue_name)
        def callback(ch, method, properties, body):
            doc = json.loads(body)
            print(" [x] Received %r" % doc)
            # не давать нов задачу пока не сделает имеющуюся
            # ch.basic_qos(prefetch_count=1)
            # Подтверждение получения сообщения. Без него сообщения будут выводиться заново после падения обработчика.
            ch.basic_ack(delivery_tag=method.delivery_tag)

        # on_message_callback=callback даже если вы убьете рабочего с помощью CTRL+C во время обработки сообщения, ничего не будет потеряно.
        # Вскоре после смерти работника все неподтвержденные сообщения будут доставлены повторно.
        channel.basic_consume(queue=queue_name, on_message_callback=callback)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
