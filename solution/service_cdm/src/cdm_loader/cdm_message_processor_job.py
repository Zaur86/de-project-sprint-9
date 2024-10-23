from datetime import datetime
from logging import Logger
from typing import Dict, List
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository import CdmRepository, OrderCdmBuilder


class CdmMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                cdm_repository: CdmRepository,
                orders_builder: OrderCdmBuilder,
                batch_size: int,
                logger: Logger) -> None:
        self._logger = logger
        self._consumer = consumer
        self._repository = cdm_repository
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            self._logger.info(f"{datetime.utcnow()}: Message received")

            order_dict = msg['payload']

            builder = OrderCdmBuilder(order_dict)

            self._load_reports(builder)

            self._logger.info(f"{datetime.utcnow()}: {dst_msg}")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
    
    def _load_reports(self, builder: OrderCdmBuilder) -> None:
        for p in builder.user_category_counters():
            self._repository.user_category_counters_insert(p)
        for p in builder.user_product_counters():
            self._repository.user_product_counters_insert(p)
