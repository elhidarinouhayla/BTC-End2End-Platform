from pwdlib import PasswordHash

password_hash = PasswordHash.recommended()

def get_hash_password(password):
    return password_hash.hash(password)

def verify_password(plained_pwd , hashed_pwd):
    return password_hash.verify(plained_pwd , hashed_pwd)