from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository, OrderDdsBuilder

class DdsMessageProcessor:
    def __init__(self, consumer: KafkaConsumer, producer: KafkaProducer,
                dds_repository: DdsRepository, dds_builder: OrderDdsBuilder,
                batch_size: 30, logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._dds_builder = dds_builder
        self._batch_size = batch_size
        self._logger = logger


    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")


        for _ in range(self._batch_size):

            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            payload = msg['payload']

            builder = self._dds_builder(payload)
            rep = self._dds_repository()

            rep.h_user_insert(builder.h_user())
            rep.h_product_insert(builder.h_product())
            rep.h_category_insert(builder.h_category())
            rep.h_restaurant_insert(builder.h_restaurant())
            rep.h_order_insert(builder.h_order())
            rep.l_order_product_insert(builder.l_order_product())
            rep.l_product_restaurant_insert(builder.l_product_restaurant())
            rep.l_product_category_insert(builder.l_product_category())
            rep.l_order_user_insert(builder.l_order_user())
            rep.s_user_names_insert(builder.s_user_names())
            rep.s_product_names_insert(builder.s_product_names())
            rep.s_restaurant_names_insert(builder.s_restaurant_names())
            rep.s_order_cost_insert(builder.s_order_cost())
            rep.s_restaurant_names_insert(builder.s_order_status())

            dds_b = self._dds_builder(msg)
            dds_b.h_user()

            user = self._dds_builder.h_user()
            self._dds_repository.h_user_insert(user)

            user_id = msg['payload']['user']['id']
            for product in msg['payload']['products']:
                product_id = product['id']
                product_name = product['name']
                category_name = product['category']


            dst_msg = {
                        "object_id": msg["object_id"],
                        "object_type": "order",
                        "payload": {
                            "user_id": user_id,
                            "product_id": product_id,
                            "product_name": product_name,
                            "category_name": category_name,
                        }
                    }
                                
            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
