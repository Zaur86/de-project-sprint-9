import uuid
from datetime import datetime
from typing import Any, Dict, List
from uuid import UUID

from lib.pg import PgConnect
from pydantic import BaseModel


class R_user_category_counters(BaseModel):
    user_id: uuid.UUID
    category_id: uuid.UUID
    category_name: str

class R_user_product_counters(BaseModel):
    user_id: uuid.UUID
    product_id: uuid.UUID
    product_name: str


class OrderCdmBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.order_ns_uuid = uuid.NAMESPACE_DNS

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))
    
    def user_category_counters(self) -> List[R_user_category_counters]:
        user_id = self._dict['user_id']
        res = []
        for prod_dict in self._dict['products']:
            cat_name = prod_dict['category']
            res.append(
                R_user_category_counters(
                    user_id=self._uuid(user_id),
                    category_id=self._uuid(cat_name),
                    category_name=cat_name
                )
            )
        return res
    
    def user_product_counters(self) -> List[R_user_product_counters]:
        user_id = self._dict['user_id']
        res = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            prod_name = prod_dict['name']
            res.append(
                R_user_product_counters(
                    user_id=self._uuid(user_id),
                    product_id=self._uuid(prod_id),
                    product_name=prod_name
                )
            )
        return res


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_category_counters_insert(self, obj: R_user_category_counters) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters(
                            user_id,
                            category_id,
                            category_name,
                            order_cnt
                        )
                        VALUES(
                            %(user_id)s,
                            %(category_id)s,
                            %(category_name)s,
                            1
                        )
                        ON CONFLICT (user_id, category_id) DO UPDATE SET
                            order_cnt = user_category_counters.order_cnt + 1;
                    """,
                    {
                        'user_id': obj.user_id,
                        'category_id': obj.category_id,
                        'category_name': obj.category_name
                    }
                )

    def user_product_counters_insert(self, obj: R_user_product_counters) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters(
                            user_id,
                            product_id,
                            product_name,
                            order_cnt
                        )
                        VALUES(
                            %(user_id)s,
                            %(product_id)s,
                            %(product_name)s,
                            1
                        )
                        ON CONFLICT (user_id, product_id) DO UPDATE SET
                            order_cnt = user_product_counters.order_cnt + 1;
                    """,
                    {
                        'user_id': obj.user_id,
                        'product_id': obj.product_id,
                        'product_name': obj.product_name
                    }
                )