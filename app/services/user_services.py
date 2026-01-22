from app.db.models.user import User
from app.utils.hashing import get_hash_password



# create new user 
def create_user(user , db):
    new_user = User(
        username = user.username,
        email = user.email,
        password_hash = get_hash_password(user.password)
    )
    
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    
    return new_user



# verify is the user exits in database
def verify_user_is_exists(user , db):
    user_query = db.query(User).filter(User.email == user.email).first()
    
    if user_query :
        return user_query
    else:
        return 
    
    
    