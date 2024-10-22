import uuid
from datetime import datetime
from typing import Any, Dict, List
from uuid import UUID

from lib.pg import PgConnect
from pydantic import BaseModel


class H_category(BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str

class H_order(BaseModel):
    h_order_pk: uuid.UUID
    order_id: int
    order_dt: datetime
    load_dt: datetime
    load_src: str

class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str

class H_restaurant(BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str

class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str

class L_order_product(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str

class L_order_user(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str

class L_product_category(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_category_pk: uuid.UUID
    load_dt: datetime
    load_src: str

class L_product_restaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    load_dt: datetime
    load_src: str

class S_order_cost(BaseModel):
    h_order_pk: uuid.UUID
    cost: float 
    payment: float 
    load_dt: datetime
    load_src: str
    hk_order_cost_hashdiff: uuid.UUID

class S_order_status(BaseModel):
    h_order_pk: uuid.UUID
    status: str 
    load_dt: datetime
    load_src: str
    hk_order_status_hashdiff: uuid.UUID 

class S_product_names(BaseModel):
    h_product_pk: uuid.UUID
    name: str 
    load_dt: datetime
    load_src: str
    hk_product_names_hashdiff: uuid.UUID 

class S_restaurant_names(BaseModel):
    h_restaurant_pk: uuid.UUID
    name: str 
    load_dt: datetime
    load_src: str
    hk_restaurant_names_hashdiff: uuid.UUID 

class S_user_names(BaseModel):
    h_user_pk: uuid.UUID
    username: str 
    userlogin: str 
    load_dt: datetime
    load_src: str
    hk_user_names_hashdiff: uuid.UUID 


class OrderDdsBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = "stg.order_events"
        self.order_ns_uuid = uuid.NAMESPACE_DNS

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))

    def h_category(self) -> List[H_category]:
        res = []
        for prod_dict in self._dict['products']:
            cat_name = prod_dict['category']
            res.append(
                H_category(
                    h_category_pk=self._uuid(cat_name),
                    category_name=cat_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return res
    
    def h_order(self) -> H_order:
        order_id = self._dict['id']
        order_dt = self._dict['date']
        return H_order(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt=order_dt,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_product(self) -> List[H_Product]:
        res = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            res.append(
                H_Product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return res
    
    def h_restaurant(self) -> H_restaurant:
        rest_id = self._dict['restaurant']['id']
        return H_restaurant(
            h_restaurant_pk=self._uuid(rest_id),
            restaurant_id=rest_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_user(self) -> H_User:
        user_id = self._dict['user']['id']
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def l_order_product(self) -> List[L_order_product]:
        order_id = self._dict['id']
        res = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            res.append(
                L_order_product(
                    hk_order_product_pk=self.\
                        _uuid(f"{order_id}#$#{prod_id}"),
                    h_order_pk=self._uuid(order_id),
                    h_product_pk=self._uuid(prod_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return res
    
    def l_order_user(self) -> L_order_user:
        order_id = self._dict['id']
        user_id = self._dict['user']['id']
        return L_order_user(
            hk_order_user_pk=self.\
                _uuid(f"{order_id}#$#{user_id}"),
            h_order_pk=self._uuid(order_id),
            h_user_pk=self._uuid(user_id),
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def l_product_category(self) -> List[L_product_category]:
        res = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            cat_name = prod_dict['category']
            res.append(
                L_product_category(
                    hk_product_category_pk=self.\
                        _uuid(f"{prod_id}#$#{cat_name}"),
                    h_product_pk=self._uuid(prod_id),
                    h_category_pk=self._uuid(cat_name),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return res
    
    def l_product_restaurant(self) -> List[L_product_restaurant]:
        res = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            rest_id = self._dict['restaurant']['id']
            res.append(
                L_product_restaurant(
                    hk_product_restaurant_pk=self.\
                        _uuid(f"{prod_id}#$#{rest_id}"),
                    h_product_pk=self._uuid(prod_id),
                    h_restaurant_pk=self._uuid(rest_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return res
    
    def s_order_cost(self) -> S_order_cost:
        order_id = self._dict['id']
        cost = self._dict['cost']
        payment = self._dict['payment']
        return S_order_cost(
            h_order_pk=self._uuid(order_id),
            cost=cost,
            payment=payment,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_cost_hashdiff=self.\
                _uuid(f"{order_id}#$#{cost}#$#{payment}")
        )
    
    def s_order_status(self) -> S_order_status:
        order_id = self._dict['id']
        status = self._dict['status']
        return S_order_status(
            h_order_pk=self._uuid(order_id),
            status=status,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_hashdiff=self.\
                _uuid(f"{order_id}#$#{status}")
        )
    
    def s_product_names(self) -> List[S_product_names]:
        res = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            prod_name = prod_dict['name']
            res.append(
                S_product_names(
                    h_product_pk=self._uuid(prod_id),
                    name=prod_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                    hk_product_names_hashdiff=self.\
                        _uuid(f"{prod_id}#$#{prod_name}")
                )
            )
        return res
    
    def s_restaurant_names(self) -> S_restaurant_names:
        rest_id = self._dict['restaurant']['id']
        rest_name = self._dict['restaurant']['name']
        return S_restaurant_names(
            h_restaurant_pk=self._uuid(rest_id),
            name=rest_name,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_hashdiff=self.\
                _uuid(f"{rest_id}#$#{rest_name}")
        )
    
    def s_user_names(self) -> S_user_names:
        user_id = self._dict['user']['id']
        user_name = self._dict['user']['name']
        user_login = self._dict['user']['login']
        return S_user_names(
            h_user_pk=self._uuid(user_id),
            username=user_name,
            userlogin=user_login,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_user_names_hashdiff=self.\
                _uuid(f"{user_id}#$#{user_name}#$#{user_login}")
        )


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
    
    def h_category_insert(self, obj: H_category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category(
                            h_category_pk,
                            category_name,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_category_pk)s,
                            %(category_name)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_category_pk) DO NOTHING;
                    """,
                    {
                        'h_category_pk': obj.h_category_pk,
                        'category_name': obj.category_name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )
    
    def h_order_insert(self, obj: H_order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(
                            h_order_pk,
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(order_dt)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'order_id': obj.order_id,
                        'order_dt': obj.order_dt,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def h_product_insert(self, obj: H_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product(
                            h_product_pk,
                            product_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(product_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': obj.h_product_pk,
                        'product_id': obj.product_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )
    
    def h_restaurant_insert(self, obj: H_restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(
                            h_restaurant_pk,
                            restaurant_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'restaurant_id': obj.restaurant_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )
    
    def h_user_insert(self, obj: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(user_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': obj.h_user_pk,
                        'user_id': obj.user_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def l_order_product_insert(self, obj: L_order_product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product(
                            hk_order_product_pk,
                            h_order_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_product_pk)s,
                            %(h_order_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_product_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_product_pk': obj.hk_order_product_pk,
                        'h_order_pk': obj.h_order_pk,
                        'h_product_pk': obj.h_product_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def l_order_user_insert(self, obj: L_order_user) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user(
                            hk_order_user_pk,
                            h_order_pk,
                            h_user_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_user_pk)s,
                            %(h_order_pk)s,
                            %(h_user_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_user_pk': obj.hk_order_user_pk,
                        'h_order_pk': obj.h_order_pk,
                        'h_user_pk': obj.h_user_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def l_product_category_insert(self, obj: L_product_category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category(
                            hk_product_category_pk,
                            h_product_pk,
                            h_category_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_category_pk)s,
                            %(h_product_pk)s,
                            %(h_category_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_category_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_category_pk': obj.hk_product_category_pk,
                        'h_product_pk': obj.h_product_pk,
                        'h_category_pk': obj.h_category_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )
    
    def l_product_restaurant_insert(self, obj: L_product_restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant(
                            hk_product_restaurant_pk,
                            h_product_pk,
                            h_restaurant_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_restaurant_pk)s,
                            %(h_product_pk)s,
                            %(h_restaurant_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_restaurant_pk': obj.hk_product_restaurant_pk,
                        'h_product_pk': obj.h_product_pk,
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def s_order_cost_insert(self, obj: S_order_cost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost(
                            h_order_pk,
                            cost,
                            payment,
                            load_dt,
                            load_src,
                            hk_order_cost_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(cost)s,
                            %(payment)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_cost_hashdiff)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'cost': obj.cost,
                        'payment': obj.payment,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_order_cost_hashdiff': obj.hk_order_cost_hashdiff
                    }
                )

    def s_order_status_insert(self, obj: S_order_status) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                            h_order_pk,
                            status,
                            load_dt,
                            load_src,
                            hk_order_status_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(status)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_status_hashdiff)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'status': obj.status,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_order_status_hashdiff': obj.hk_order_status_hashdiff
                    }
                )

    def s_product_names_insert(self, obj: S_product_names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names(
                            h_product_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_product_names_hashdiff
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_product_names_hashdiff)s
                        )
                        ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': obj.h_product_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_product_names_hashdiff': obj.hk_product_names_hashdiff
                    }
                )

    def s_restaurant_names_insert(self, obj: S_restaurant_names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(
                            h_restaurant_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_restaurant_names_hashdiff
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_restaurant_names_hashdiff)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_restaurant_names_hashdiff': obj.hk_restaurant_names_hashdiff
                    }
                )

    def s_user_names_insert(self, obj: S_user_names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names(
                            h_user_pk,
                            username,
                            userlogin,
                            load_dt,
                            load_src,
                            hk_user_names_hashdiff
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(username)s,
                            %(userlogin)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_user_names_hashdiff)s
                        )
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': obj.h_user_pk,
                        'username': obj.username,
                        'userlogin': obj.userlogin,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_user_names_hashdiff': obj.hk_user_names_hashdiff
                    }
                )