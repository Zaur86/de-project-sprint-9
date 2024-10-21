from datetime import datetime
from logging import Logger
from uuid import UUID
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from cdm_loader.repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                producer: KafkaProducer,
                cdm_repository: CdmRepository,
                batch_size: int,
                logger: Logger) -> None:
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            self._logger.info(f"{datetime.utcnow()}: Message received")

            for prd_item in msg["products"]:
                self._cdm_repository.user_category_counters(msg["user_id"], prd_item["category"])
                self._cdm_repository.user_product_counters(msg["product_id"], prd_item["category"])

        self._logger.info(f"{datetime.utcnow()}: FINISH")
