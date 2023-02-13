from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote  

#SQLALCHEMY_DATABASE_URL = 'mysql://magik:%s@172.18.190.220:3306/TNM_Autopilot'% quote('ThisIsPwd@1')
#SQLALCHEMY_DATABASE_URL = 'mysql+mysqlconnector://cvmuser:Zxcmnb123@144.24.148.1/robox'
SQLALCHEMY_DATABASE_URL = 'mysql+mysqlconnector://root:%s@localhost:3306/AUTOPILOT'% quote('Zxcmnb123')

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
