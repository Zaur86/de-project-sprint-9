from datetime import datetime
from logging import Logger
from uuid import UUID
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository
from dds_loader.repository import OrderDdsBuilder


class DdsMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                producer: KafkaProducer,
                dds_repository: DdsRepository,
                orders_builder: OrderDdsBuilder,
                batch_size: int,
                logger: Logger) -> None:
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._orders_builder = orders_builder
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            self._logger.info(f"{datetime.utcnow()}: Message received")
        
            order = self._orders_builder(msg['payload'])

            self._dds_repository.h_user_insert(order.h_user())
            self._dds_repository.h_order_insert(order.h_order())
            self._dds_repository.h_restaurant_insert(order.h_restaurant())
            self._dds_repository.l_order_user_insert(order.l_order_user())
            self._dds_repository.s_order_cost_insert(order.s_order_cost())
            self._dds_repository.s_order_status_insert(order.s_order_status())
            self._dds_repository.s_restaurant_names_insert(order.s_restaurant_names())
            self._dds_repository.s_user_names_insert(order.s_user_names())

            [self._dds_repository.h_category_insert(x) for x in order.h_category()]
            [self._dds_repository.h_product_insert(x) for x in order.h_product()]
            [self._dds_repository.l_order_product_insert(x) for x in order.l_order_product()]
            [self._dds_repository.l_product_category_insert(x) for x in order.l_product_category()]
            [self._dds_repository.l_product_restaurant_insert(x) for x in order.l_product_restaurant()]
            [self._dds_repository.s_product_names_insert(x) for x in order.s_product_names()]
            
            dst_msg = {
                "user_id": msg['payload']['user']['id'],
                "products": msg['payload']['products']
            }

            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")
            
        self._logger.info(f"{datetime.utcnow()}: FINISH")
