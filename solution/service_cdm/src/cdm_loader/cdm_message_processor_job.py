from datetime import datetime
from logging import Logger
from uuid import UUID
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository import CdmRepository
from cdm_loader.repository import OrderCdmBuilder


class CdmMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                cdm_repository: CdmRepository,
                orders_builder: OrderCdmBuilder,
                batch_size: int,
                logger: Logger) -> None:
        self._logger = logger
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._orders_builder = orders_builder
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            self._logger.info(f"{datetime.utcnow()}: Message received")

            order = self._orders_builder(msg)

            [self._cdm_repository.user_category_counters_insert(x) for x in order.user_category_counters()]
            [self._cdm_repository.user_product_counters_insert(x) for x in order.user_product_counters()]

        self._logger.info(f"{datetime.utcnow()}: FINISH")
