import logging
from utils import jwt_token
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from domain import UserModel, ValidateToken
from http_client import http_client
from config import config as router_config
from db_sessions import main_db_instance
from dependencies.validation import validate_token_dependency
import httpx

logger = logging.getLogger('USER_INFO_ROUTER')

router = APIRouter(
    tags=['user_info'],
    responses={404: {'description': 'Not found'}}
    # dependencies=[Depends(validate_token_dependency)]
)

async def validate_firebase_token(firebase_token):
    async with httpx.AsyncClient() as client:
        res = await client.post(
            f"https://identitytoolkit.googleapis.com/v1/accounts:lookup?key={router_config.firebase_api_key}", data={
                "idToken": firebase_token
            })
    return res


@router.post("/validate_token")
async def validate_token(user: ValidateToken):
    # res = await http_client.post(
    #     f"https://identitytoolkit.googleapis.com/v1/accounts:lookup?key={router_config.firebase_api_key}", data={
    #         "idToken": user.token
    #     })
    # httpx test - delete above if this works
    # async with httpx.AsyncClient() as client:
    #     res = await client.post(f"https://identitytoolkit.googleapis.com/v1/accounts:lookup?key={router_config.firebase_api_key}", data={
    #         "idToken": user.token
    #     })
    res = await validate_firebase_token(user.token)
    data = res.json()

    logger.info(f'users email logging  is - {user.email.lower()} and status is {res.status_code}' )
    if res.status_code == 200 and user.email.lower() == str(data['users'][0]['email']).lower():
        email = data['users'][0]['email'].lower()

        if user.email.lower() != email:
            return JSONResponse({"success": False, "message": "Error logging in. Email does not match"}, status_code=500)


        query_statement = f"""select token from company.user_profile where lower(email) = '{email}'"""
        rows = await main_db_instance.fetch_rows(query_statement)

        logger.info(f"sql queiries match. moving on. email is {email}")
        logger.info(f'query results are {rows}')


        if len(rows) != 0:
            result_data_json = jsonable_encoder(rows[0])
            return result_data_json
        else:
            logger.info(f'{email} token not found. Creating user')
            return await new_token(user)
            # return JSONResponse({"success": False, "message": "Error user not found."}, status_code=500)
    else:
        return JSONResponse({"success": False, "message": "Error token or email not valid."}, status_code=422)


@router.post("/newToken")
async def new_token(user: ValidateToken):
    # TODO: this only generates a new token, we should have the ability to change the token
    #  if the token is every compromised.

    # res = await http_client.post(
    #     f"https://identitytoolkit.googleapis.com/v1/accounts:lookup?key={router_config.firebase_api_key}", data={
    #         "idToken": user.token
    #     })
    res = await validate_firebase_token(user.token)
    data = res.json()

    if res.status_code == 200 and user.email.lower() == str(data['users'][0]['email']).lower():  # and "idToken" in data:
        try:
            token = jwt_token(user.email)
            query_statement = f"insert into company.user_profile (email, token) values('{user.email}', '{token}');"
            await main_db_instance.execute(query_statement)
            return {"success": True, "token": token}

        except Exception as exp:
            logger.exception(f'Exception while generating a new token, {exp}')
            return JSONResponse({"success": False, "message": "Error saving data."}, status_code=500)

    else:
        return JSONResponse({"success": False, "message": "Error signing up.", "code": res.status},
                            status_code=res.status)


@router.post("/updateinfo")
async def update_info(user: UserModel):
# async def update_info(user: UserModel, Depends=validate_token_dependency):
    # TODO this only generates a new token, we should have the ability to change the token
    #  if the token is every compromised.
    print("Testig")
    res = await validate_firebase_token(user.token)
    data = res.json()

    if res.status_code == 200 and user.email.lower() == str(data['users'][0]['email']).lower():  # and "idToken" in data:
        logger.info("Successful message and emails match")
        try:
            query_statement = f"UPDATE company.user_profile SET firstName = '{user.firstName}', lastName = '{user.lastName}' WHERE email = '{user.email}';"
            await main_db_instance.execute(query_statement)
            return {"success": True}

        except Exception as exp:
            logging.exception(f'{exp}')
            return JSONResponse({"success": False, "message": "Error saving data."}, status_code=500)

    else:
        return JSONResponse({"success": False, "message": "Error signing up.", "code": res.status},
                            status_code=res.status)


@router.post("/getuserinfo")
async def get_user_info(user: ValidateToken):
#async def get_user_info(user: ValidateToken, Depends=validate_token_dependency):
    # TODO this only generates a new token, we should have the ability to change the token
    #  if the token is every comprimised.
    logger.info("we are in the getuser info endpoint")
    try:
        query_statement = f"select firstname, lastname, email, token from company.user_profile where token = '{user.token}' and email = '{user.email}';"
        result_data = await main_db_instance.fetch_rows(query_statement)

        logger.info(f"results are {result_data}")
        if len(result_data) != 0:
            result_data = jsonable_encoder(result_data)
            return {"success": True, "data": result_data}

        else:
            return JSONResponse({"success": False, "message": "User & Token mismatch "}, status_code=500)
    except:
        return JSONResponse({"success": False, "message": "Error getting profile data. Check email/token"},
                            status_code=500)

