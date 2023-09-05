from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository import CdmRepository 



class CdmMessageProcessor:
    def __init__(self, consumer: KafkaConsumer,
                cdm_repository: CdmRepository, 
                batch_size: 100, logger: Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):

            dt = datetime.utcnow()
            dt = dt.strftime('%Y-%m-%d %H:%M:%S')

            msg = self._consumer.consume()

            if not msg:
                print(msg)
                break
            user_id = msg['payload']['user_id']
            product_id = msg['payload']['product_id']
            product_name = msg['payload']['product_name']
            category_name = msg['payload']['category_name']


            self._cdm_repository.insert_user_product_counters(user_id, product_id, product_name)
            self._cdm_repository.insert_user_category_counters(user_id, category_name)           

        self._logger.info(f"{datetime.utcnow()}: FINISH")
