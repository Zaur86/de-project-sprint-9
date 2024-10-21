from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer
from lib.kafka_connect import KafkaProducer
from dds_loader.repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                producer: KafkaProducer,
                dds_repository: DdsRepository,
                batch_size: int,
                logger: Logger) -> None:
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            self._logger.info(f"{datetime.utcnow()}: Message received")
        
            order = msg['payload']

            self._dds_repository.h_user_insert(order["user"]["user_id"])
            self._dds_repository.h_order_insert(order["id"], order["date"])
            self._dds_repository.h_restaurant_insert(order["id"], order["date"])
            self._dds_repository.l_order_user_insert(order["id"], order["user"]["user_id"])
            self._dds_repository.s_order_cost_insert(order["id"], order["cost"], order["payment"])
            self._dds_repository.s_order_status_insert(order["id"], order["status"])
            self._dds_repository.s_restaurant_names_insert(order["restaurant"]["id"], order["restaurant"]["name"])
            self._dds_repository.s_user_names_insert(order["user"]["id"], order["user"]["name"], order["user"]["login"])

            dst_msg = {
                "user_id": order["user"]["user_id"],
                "products": []
            }

            for prd_item in order["products"]:
                self._dds_repository.h_category_insert(prd_item["category"])
                self._dds_repository.h_category_insert(prd_item["id"])
                self._dds_repository.l_order_product_insert(order["id"], prd_item["id"])
                self._dds_repository.l_product_category_insert(prd_item["id"], prd_item["category"])
                self._dds_repository.l_product_restaurant_insert(prd_item["id"], order["restaurant"]["id"])
                self._dds_repository.s_product_names_insert(prd_item["id"], prd_item["name"])

                dst_msg["products"].append({"product_id": prd_item["id"], "category": prd_item["category"]})

                self._producer.produce(dst_msg)
                self._logger.info(f"{datetime.utcnow()}. Message Sent")
            
        self._logger.info(f"{datetime.utcnow()}: FINISH")
