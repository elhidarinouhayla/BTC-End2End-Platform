from fastapi import FastAPI
from app.db.database import engine
from app.db.models.base import Base
from app.api.v1.routes.auth import auth_router




app = FastAPI()


Base.metadata.create_all(engine)

app.include_router(auth_router , prefix='/api/v1')
