import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
#from pydantic import BaseModel



class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(
            self,
            user_id: str,
    ) -> None:
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
                    ON CONFLICT (user_id) DO NOTHING
                    ;
                """,
                    {
                        'h_user_pk': str(uuid.uuid5(uuid.NAMESPACE_DNS, user_id)),
                        'user_id': user_id,
                        'load_dt': datetime.utcnow().strftime("%y-%m-%d %H:%M:%S"),
                        'load_src': 'stg.order_events'
                    }
                )
    
    def h_order_insert(
            self,
            order_id: str,
            order_dt: datetime
    ) -> None:
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
                    ON CONFLICT (order_id) DO UPDATE
                    SET
                        order_dt = EXCLUDED.order_dt,
                        load_dt = EXCLUDED.load_dt
                    ;
                """,
                    {
                        'h_order_pk': str(uuid.uuid5(uuid.NAMESPACE_DNS, order_id)),
                        'order_id': order_id,
                        'order_dt': order_dt,
                        'load_dt': datetime.utcnow().strftime("%y-%m-%d %H:%M:%S"),
                        'load_src': 'stg.order_events'
                    }
                )
                 
    def h_restaurant_insert(
            self,
            restaurant_id: str,
    ) -> None:
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
                    ON CONFLICT (restaurant_id) DO NOTHING
                    ;
                """,
                    {
                        'h_restaurant_pk': str(uuid.uuid5(uuid.NAMESPACE_DNS, restaurant_id)),
                        'restaurant_id': restaurant_id,
                        'load_dt': datetime.utcnow().strftime("%y-%m-%d %H:%M:%S"),
                        'load_src': 'stg.order_events'
                    }
                )
    
    def l_order_user_insert(
            self,
            order_id: str,
            user_id: datetime
    ) -> None:
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
                    select 
                            
                    VALUES(
                        %(hk_order_user_pk)s,
                        %(h_order_pk)s,
                        %(h_user_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_order_pk, h_user_pk) DO UPDATE
                    SET
                        load_dt = EXCLUDED.load_dt
                    ;
                """,
                    {
                        'hk_order_user_pk': str(uuid.uuid5(uuid.NAMESPACE_DNS, order_id)),
                        'h_order_pk': order_id,
                        'h_user_pk': order_dt,
                        'load_dt': datetime.utcnow().strftime("%y-%m-%d %H:%M:%S"),
                        'load_src': 'stg.order_events'
                    }
                )