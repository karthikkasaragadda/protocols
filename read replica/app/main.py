import os
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import sessionmaker, Session, declarative_base

DB_USER = os.getenv("DB_USER", "myuser")
DB_PASS = os.getenv("DB_PASSWORD", "mypassword")
DB_NAME = os.getenv("DB_NAME", "mydb")

MASTER_URL = f"postgresql://{DB_USER}:{DB_PASS}@db-master:5432/{DB_NAME}"
REPLICA_URL = f"postgresql://{DB_USER}:{DB_PASS}@db-replica:5432/{DB_NAME}"

master_engine = create_engine(MASTER_URL)
replica_engine = create_engine(REPLICA_URL)

MasterSession = sessionmaker(bind=master_engine)
ReplicaSession = sessionmaker(bind=replica_engine)

Base = declarative_base()

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    item_name = Column(String)

app = FastAPI()

def get_master_db():
    db = MasterSession()
    try:
        yield db
    finally:
        db.close()

def get_replica_db():
    db = ReplicaSession()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=master_engine)

@app.get("/orders")
async def get_orders(db: Session = Depends(get_replica_db)):
    orders = db.query(Order).all()
    return {"msg": "all records in db", "orders": orders}

@app.post("/orders")
async def create_order(name: str, db: Session = Depends(get_master_db)):
    order = Order(item_name = name)
    db.add(order)
    db.commit()
    db.refresh(order)
    return {"msg": "added order", "order": order}



