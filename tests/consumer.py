import re
import pika
import json
import time
from CONFIG import Config as cfg
import os
import sys



class Rabbit:
    """Класс для работы с очередями на случай использования в проект брокера сообщений"""

    def create_connection(self):
        credentials = pika.PlainCredentials(cfg.LOGIN, cfg.PWD)
        parameters = pika.ConnectionParameters(cfg.HOST, cfg.PORT, cfg.PATH, credentials)
        connection = pika.BlockingConnection(parameters)
        return connection

    def consumer_queue(self, queue_name):
        channel = self.create_connection().channel()
        # Создается очередь.устойчивая очередь к падению сервера с rabbit mq
        # channel.queue_declare(queue=queue_name, durable=True)

        def callback(ch, method, properties, body):
            time.sleep(3)
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

    def consumer_exchange(self, exchange):
        channel = self.create_connection().channel()
        channel.exchange_declare(exchange=exchange, exchange_type='direct')
        result = channel.queue_declare(queue='hello',
                                       durable=True)  # exclusive=True очередь будет удалена
        queue_name = result.method.queue_response
        channel.queue_bind(exchange=exchange, queue=queue_name, routing_key='hello')

        def callback(ch, method, properties, body):
            time.sleep(3)
            # with open(r"D:\Robots\CB_Credit\For tests\картинка.png", "wb") as file:
            #     file.write(base64.b64decode(body))
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


if __name__ == '__main__':
    try:
        consumer = Rabbit()
        consumer.consumer_queue('goodbay')
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

