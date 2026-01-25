
from app.db.database import sessionLocal


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()