import logging
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from domain import OobCodeModel, UserModel
from http_client import http_client
from config import config as app_config
from db_sessions import main_db_instance

logger = logging.getLogger('USER_ROUTER')

router = APIRouter(
    tags=['user'],
    responses={404: {'description': 'Not found'}}
)


@router.get("/verify-email")
async def verify_email(ve_model: OobCodeModel):
    if ve_model.mode == 'verifyEmail' and ve_model.oob_code is not None:
        res = await http_client.post(
            f"https://identitytoolkit.googleapis.com/v1/accounts:update?key={app_config.firebase_api_key}", data={
                "oobCode": ve_model.oob_code
            })
        if res.status == 200:
            return {"success": True, "message": "Email successfully verified."}
        else:
            return {"success": False, "message": "Error verifying email."}
    else:
        return JSONResponse({"success": False, "message": "Unprocessable request"}, status_code=422)


@router.post("/login")
async def login(user: UserModel):
    res = await http_client.post(
        f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={app_config.firebase_api_key}",
        data={
            "email": user.email, "password": user.password, "returnSecureToken": True
        })

    if res.status == 200:
        query_statement = "select token from company.user_profile where lower(email) = $1"
        rows = await main_db_instance.fetch_rows(query_statement, user.email.lower())

        if len(rows) != 0:
            return jsonable_encoder(rows[0])
        else:
            return JSONResponse({"success": False, "message": "Error logging in."}, status_code=500)
    else:
        return JSONResponse({"success": False, "message": "Error logging in."}, status_code=422)

