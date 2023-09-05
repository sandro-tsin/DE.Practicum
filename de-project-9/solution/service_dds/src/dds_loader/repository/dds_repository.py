import uuid
from lib.pg import PgConnect
from datetime import datetime

from typing import Any, Dict, List
from pydantic import BaseModel


class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str


class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str 

class H_Category(BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str

class H_Restaurant(BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str 


class H_Order(BaseModel):
    h_order_pk: uuid.UUID
    order_id: str
    order_dt: str
    load_dt: datetime
    load_src: str 

class L_OrderProduct(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str 

class L_ProductRestaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    load_dt: datetime
    load_src: str 

class L_ProductСategory(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    load_dt: datetime
    load_src: str 

class L_OrderUser(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str 

class S_UserNames (BaseModel):
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str 
    hk_user_names_hashdiff: uuid.UUID

class S_ProductNames (BaseModel):
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str 
    hk_product_names_hashdiff: uuid.UUID

class S_RestaurantNames (BaseModel):
    h_restaurant_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str 
    hk_restaurant_names_hashdiff: uuid.UUID

class S_OrderCost (BaseModel):
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str 
    hk_restaurant_names_hashdiff: uuid.UUID   

class S_OrderStatus (BaseModel):
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str 
    hk_order_status_hashdiff: uuid.UUID   


class OrderDdsBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = "stg-service-orders"
        self.order_ns_uuid = uuid.UUID('')

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))

    def h_user(self) -> H_User:
        user_id = self._dict['user']['id']
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_product(self) -> List[H_Product]:
        products = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                H_Product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return products

    def h_category(self) -> List[H_Category]:
        category = []

        for prod_dict in self._dict['products']:
            category_name = prod_dict['category']
            category.append(
                H_Category(
                    h_product_pk=self._uuid(category_name),
                    category_name=category_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return category
  
    def h_restaurant(self) -> List[H_Restaurant]:
        restaurant_id = self._dict['restaurant']['id']
        return H_Restaurant(
            h_user_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
   
    def h_order(self) -> List[H_Order]:
        order_id = self._dict['id']
        order_dt = self._dict['date']
        return H_Order(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt=order_dt,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def l_order_product(self) -> List[L_OrderProduct]:

        order_id = self._dict['id']
        order_product = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            order_product.append(
                H_Product(
                    hk_order_product_pk=self._uuid(order_id+prod_id),
                    h_order_pk=self._uuid(order_id),
                    h_product_pk=self._uuid(prod_id),    
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return order_product
    
    def l_product_restaurant(self) -> List[L_ProductRestaurant]:
        product_restaurant = []
        restaurant_id = self._dict['restaurant']['id']

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']

            product_restaurant.append(
                L_ProductRestaurant(
                    hk_product_restaurant_pk=self._uuid(restaurant_id+prod_id),
                    h_product_pk=self._uuid(prod_id),
                    h_restaurant_pk=self._uuid(restaurant_id),    
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return product_restaurant

    def l_product_category(self) -> List[L_ProductСategory]:
        product_category = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            category_name = prod_dict['category']

            product_category.append(
                L_ProductСategory(
                    hk_product_category_pk=self._uuid(category_name+prod_id),
                    h_product_pk=self._uuid(prod_id),
                    h_category_pk=self._uuid(category_name),    
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return product_category

    def l_order_user(self) -> List[L_OrderUser]:
        order_id = self._dict['id']
        user_id = self._dict['user']['id']

        return L_OrderUser(
                hk_order_user_pk=self._uuid(order_id+user_id),
                h_order_pk=self._uuid(order_id),
                h_user_pk=self._uuid(user_id),    
                load_dt=datetime.utcnow(),
                load_src=self.source_system
            )

    def s_user_names(self) -> List[S_UserNames]:
        user_id = self._dict['user']['id']
        user_name = self._dict['user']['name']

        return S_UserNames(
                h_user_pk=self._uuid(user_id),
                username=user_name,
                userlogin=user_id,    
                load_dt=datetime.utcnow(),
                load_src=self.source_system,
                hk_user_names_hashdiff=self._uuid(user_id+str(datetime.utcnow()))        
            )

    def s_product_names(self) -> List[S_ProductNames]:
        product_names = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            product_name = prod_dict['name']

            product_names.append(
                S_ProductNames(
                    h_product_pk=self._uuid(prod_id),
                    name=product_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                    hk_product_names_hashdiff=self._uuid(prod_id+str(datetime.utcnow()))        
                )
            )
        return product_names

    def s_restaurant_names(self) -> List[S_RestaurantNames]:
        restaurant_id = self._dict['restaurant']['id']
        restaurant_name = self._dict['restaurant']['name']

        return S_RestaurantNames(
                h_restaurant_pk=self._uuid(restaurant_id),
                name=restaurant_name,
                load_dt=datetime.utcnow(),
                load_src=self.source_system,
                hk_restaurant_names_hashdiff=self._uuid(restaurant_id+str(datetime.utcnow()))        
            )

    def s_order_cost(self) -> List[S_OrderCost]:
        order_id = self._dict['id']
        payment = self._dict['payment']
        cost = self._dict['cost']

        return S_OrderCost(
                h_order_pk=self._uuid(order_id),
                cost=cost,
                payment=payment,
                load_dt=datetime.utcnow(),
                load_src=self.source_system,
                hk_order_cost_hashdiff=self._uuid(order_id+str(datetime.utcnow()))        
            )

    def s_order_status(self) -> List[S_OrderStatus]:
        order_id = self._dict['id']
        status = self._dict['status']

        return S_OrderStatus(
                h_ordh_order_pker_pk=self._uuid(order_id),
                status=status,
                load_dt=datetime.utcnow(),
                load_src=self.source_system,
                hk_order_status_hashdiff=self._uuid(order_id+str(datetime.utcnow()))        
            )
    
class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(self, user: H_User) -> None:
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
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.user_id,
                        'load_dt': user.load_dt,
                        'load_src': user.load_src
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

    def h_category_insert(self, obj: H_Category) -> None:
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
                        'h_product_pk': obj.h_category_pk,
                        'product_id': obj.category_name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def h_restaurant_insert(self, obj: H_Restaurant) -> None:
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
                        'h_product_pk': obj.h_restaurant_pk,
                        'product_id': obj.restaurant_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def h_order_insert(self, obj: H_Order) -> None:
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
                            'h_product_pk': obj.h_restaurant_pk,
                            'product_id': obj.restaurant_id,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src
                        }
                    )

    def l_order_product_insert(self, obj: L_OrderProduct) -> None:
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

    def l_product_restaurant_insert(self, obj: L_ProductRestaurant) -> None:
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

    def l_product_category_insert(self, obj: L_ProductСategory) -> None:
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

    def l_order_user_insert(self, obj: L_OrderUser) -> None:
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

    def s_user_names_insert(self, obj: S_UserNames) -> None:
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
                            ON CONFLICT (hk_user_names_hashdiff) DO NOTHING;
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

    def s_product_names_insert(self, obj: S_ProductNames) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.s_product_names(
                                h_product_pk,
                                name,
                                userlogin,
                                load_dt,
                                load_src,
                                hk_product_names_hashdiff
                            )
                            VALUES(
                                %(h_product_pk)s,
                                %(name)s,
                                %(userlogin)s,
                                %(load_dt)s,
                                %(load_src)s,
                                %(hk_product_names_hashdiff)s
                            )
                            ON CONFLICT (hk_product_names_hashdiff) DO NOTHING;
                        """,
                        {
                            'h_product_pk': obj.h_product_pk,
                            'username': obj.username,
                            'userlogin': obj.name,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src,
                            'hk_product_names_hashdiff': obj.hk_product_names_hashdiff
                        }
                    )

    def s_restaurant_names_insert(self, obj: S_RestaurantNames) -> None:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.s_product_names(
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
                            ON CONFLICT (hk_restaurant_names_hashdiff) DO NOTHING;
                        """,
                        {
                            'h_restaurant_pk': obj.h_restaurant_pk,
                            'name': obj.username,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src,
                            'hk_restaurant_names_hashdiff': obj.hk_restaurant_names_hashdiff
                        }
                    )

    def s_order_cost_insert(self, obj: S_OrderCost) -> None:
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
                            ON CONFLICT (hk_order_cost_hashdiff) DO NOTHING;
                        """,
                        {
                            'h_order_pk': obj.h_order_pk,
                            'cost': obj.cost,
                            'payment': obj.payment,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src,
                            'hk_restaurant_names_hashdiff': obj.hk_order_cost_hashdiff
                        }
                    )

    def s_order_status_insert(self, obj: S_OrderStatus) -> None:
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
                            ON CONFLICT (hk_order_status_hashdiff) DO NOTHING;
                        """,
                        {
                            'h_order_pk': obj.h_order_pk,
                            'status': obj.status,
                            'load_dt': obj.load_dt,
                            'load_src': obj.load_src,
                            'hk_order_status_hashdiff': obj.hk_order_status_hashdiff
                        }
                    )
