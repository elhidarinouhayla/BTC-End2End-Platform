from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE = os.getenv("DATABASE")
DB_USER=os.getenv("DB_USER")
PASSWORD=os.getenv("PASSWORD")
PORT=os.getenv("PORT")
HOST=os.getenv("HOST")

DATABASE_URI = f'postgresql+psycopg2://{DB_USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'

engine = create_engine(DATABASE_URI)
sessionLocal = sessionmaker(bind=engine)