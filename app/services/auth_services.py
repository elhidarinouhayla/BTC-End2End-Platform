from jose import jwt , JWTError
from dotenv import load_dotenv
from app.db.models.user import User
from datetime import datetime , timedelta
import os
from fastapi.security import HTTPAuthorizationCredentials , HTTPBearer
from fastapi import Depends , HTTPException , status
from app.api.v1.dependencies import get_db
load_dotenv()

JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY')
JWT_EXPIRATION_MINUTES = int(os.getenv('JWT_EXPIRATION_MINUTES',30))

bearer_scheme = HTTPBearer()



# create token
def create_token(payload):
    
    to_encode = payload.copy()
    expire = datetime.utcnow() + timedelta(minutes=JWT_EXPIRATION_MINUTES)
    
    to_encode.update({
        "exp":expire,
        "iat":datetime.utcnow()
    })
    
    token = jwt.encode(to_encode , JWT_SECRET_KEY)
    return token


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme), db=Depends(get_db)):
    token = credentials.credentials
    
    try:
        payload = jwt.decode(token , JWT_SECRET_KEY )
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="token invalide ou expire",
        )