import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import contextmanager

class OrderCreate(BaseModel):
    customer_id: int
    total_amount: float
    status: str
    payment_method: str


master_db = "postgresql://<>@localhost:5432/postgres"
read_replica = "postgresql://<>@localhost:5433/postgres"

app = FastAPI()

@contextmanager
def get_db_conn(dsn, is_write=False):

    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        yield cur
        if is_write:
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()


@app.on_event("startup")
def startup_db_check():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INT NOT NULL,
        total_amount DECIMAL(10, 2) NOT NULL,
        status VARCHAR(20),
        payment_method VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    with get_db_conn(master_db, is_write=True) as cur:
        cur.execute(create_table_query)
        print("db created successfully")
        


@app.get("/")
async def health():
    return {"status": 200, "message": "OK"}

@app.post("/orders")
async def create_order(order: OrderCreate):
    try:
        query = """
        INSERT INTO orders (customer_id, total_amount, status, payment_method)
        VALUES (%s, %s, %s, %s)
        RETURNING order_id, created_at;
        """
        
        with get_db_conn(master_db, is_write=True) as cur:
            cur.execute(query, (
                order.customer_id, 
                order.total_amount, 
                order.status, 
                order.payment_method
            ))
            new_order = cur.fetchone()
            
        return {
            "message": "order created on master",
            "data": new_order
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/orders")
async def get_orders():
    try:
        query = "SELECT * FROM orders ORDER BY order_id DESC LIMIT 20;"
        with get_db_conn(read_replica, is_write=False) as cur:
            cur.execute(query)
            orders = cur.fetchall()
            
        return {
            "data": orders
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))