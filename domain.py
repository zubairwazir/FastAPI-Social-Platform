from pydantic import BaseModel
from typing import List


class ModelItem(BaseModel):
    score_field: str
    group_fields: List[str]
    show_fields_1: List[str]
    show_fields_2: List[str]
    year: str


class PortfolioItems(BaseModel):
    tickers: List[str]


class UserModel(BaseModel):
    email: str
    token: str
    firstName: str
    lastName: str


class OobCodeModel(BaseModel):
    mode: str
    oob_code: str


class ChangePasswordModel(BaseModel):
    current_password: str
    new_password: str


class EmailModel(BaseModel):
    email: str


class NewPasswordModel(BaseModel):
    mode: str
    oob_code: str
    new_password: str


class ValidateToken(BaseModel):
    token: str
    email: str
