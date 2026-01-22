from pydantic import BaseModel , EmailStr
from datetime import datetime

# Register

class UserBase(BaseModel):
    username: str
    email: EmailStr
    
class UserCreate(UserBase):
    password: str
    
class UserResponse(UserBase):
    id: int
    createdAt : datetime
    
    class Config:
        from_attributes = True
        
# login

class LoginRequest(BaseModel):
    email: EmailStr
    password: str
    
    
class TokenResponse(BaseModel):
    access_token: str
    token_type: str = 'bearer'