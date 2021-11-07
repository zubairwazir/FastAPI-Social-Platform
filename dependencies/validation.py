from fastapi import HTTPException, Header
from utils import validate_token


def validate_token_dependency(authorization: str = Header('')):
    is_valid, _ = validate_token(authorization)
    if not is_valid:
        raise HTTPException(status_code=401, detail='Invalid token')
