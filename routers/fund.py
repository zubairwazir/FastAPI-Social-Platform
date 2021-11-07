import logging
from fastapi import APIRouter, Header, Depends, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from utils import validate_token
from db_sessions import main_db_instance
from pydantic import BaseModel
from dependencies.validation import validate_token_dependency

logger = logging.getLogger('FUND_ROUTER')


class BaseRequest(BaseModel):
    ticker: str


class FundAddRequest(BaseRequest):
    shares: int


class FundDeleteRequest(BaseRequest):
    pass


class FundHoldingCheckRequest(BaseRequest):
    pass


async def get_user_info(authorization: str = Header('')):
    _, data = validate_token(authorization)
    user_info = await main_db_instance.fetch_rows(
        "select userid, email, client_type from company.user_profile where email = $1",
        data['email']
    )

    if len(user_info) == 0:
        raise HTTPException(status_code=404, detail='User not found')

    return dict(user_info[0].items())


router = APIRouter(
    prefix="/user/fund",
    tags=['portfolio', 'user'],
    responses={404: {'description': 'Not found'}},
    dependencies=[Depends(validate_token_dependency)]
)


# add a ticker to the user fund
@router.put('')
async def add_to_fund(body: FundAddRequest, user_info: dict = Depends(get_user_info)):
    try:
        email, user_id = user_info['email'], user_info['userid']
        ticker, nshares = body.ticker.upper(), body.shares

        # get the fund id
        fund_details = await main_db_instance.fetch_rows(
            f"SELECT fund_id from company.user_fund_details where user_id=$1",
            user_id
        )

        if len(fund_details) == 0:
            fund_details = await main_db_instance.fetch_rows(f"INSERT INTO company.user_fund_details"
                                                             f" (user_id, fund_name, fund_description)"
                                                             f" VALUES ({user_id}, 'test fund', 'test description')"
                                                             f" RETURNING *")

        # insert the ticker to user holdings
        res = await main_db_instance.fetch_rows(f"INSERT INTO company.user_fund_holding (fund_id, ticker, shares_num)"
                                                f" VALUES ({fund_details[0]['fund_id']}, '{ticker}', {nshares})"
                                                f" RETURNING *")

        return JSONResponse(jsonable_encoder(res), status_code=200)

    except Exception as exp:
        logger.exception(f'Exception while inserting the ticker {body.ticker}: {exp}')
        return JSONResponse(status_code=500, content="Error inserting the ticker")


# delete a ticker from the user fund
@router.delete('')
async def delete_from_fund(body: FundHoldingCheckRequest, user_info: dict = Depends(get_user_info)):
    try:
        email, user_id = user_info['email'], user_info['userid']
        ticker = body.ticker.upper()

        # get the fund id
        fund_details = await main_db_instance.fetch_rows(
            f"SELECT fund_id from company.user_fund_details where user_id=$1",
            user_id
        )

        # delete the ticker from the user holdings
        res = await main_db_instance.fetch_rows(f"DELETE FROM company.user_fund_holding"
                                                f" WHERE fund_id='{fund_details[0]['fund_id']}' AND ticker='{ticker}'"
                                                f" RETURNING *")

        return JSONResponse(jsonable_encoder(res))

    except Exception as exp:
        logger.exception(f'Exception while deleting the ticker {body.ticker}: {exp}')
        return JSONResponse(status_code=500, content="Error deleting the ticker")


# check if company exists in fund
@router.get('/exist/{ticker}')
async def check_if_fund_holding_exists(ticker: str, user_info: dict = Depends(get_user_info)):
    logger.info(f"Checking if the {ticker} investment exists")

    try:
        ticker = ticker.upper()
        email, user_id = user_info['email'], user_info['userid']

        # does holding exist
        query = f"""
        select count(*) 
        from company.user_fund_holding ufh, company.user_fund_details ufd, company.user_profile up 
        where ufd.fund_id = ufh.fund_id 
        and up.userid = ufd.user_id 
        and up.userid = $1
        and ufh.ticker = $2
        """
        fund_holding = await main_db_instance.fetch_rows(query, user_id, ticker)
        count = fund_holding[0]['count']

        logger.info(f'Found {count} {ticker} holding(s) for user {user_info["email"]}')
        return {'exists': count != 0}

    except Exception as exp:
        logger.exception(f'Exception while looking up the ticker {ticker} existence: {exp}')
        return JSONResponse(status_code=500, content="Error looking up the ticker")

# get all fund holdings for user
@router.get('/')
async def get_user_holdings(user_info: dict = Depends(get_user_info)):
    logger.info(f"getting {user_info['email']} investment holdings")

    try:
        email, user_id = user_info['email'], user_info['userid']

        # does holding exist
        query = f"""
        select ticker, shares_num, fund_name, fund_description
        from company.user_fund_holding ufh, company.user_fund_details ufd, company.user_profile up 
        where ufd.fund_id = ufh.fund_id 
        and up.userid = ufd.user_id 
        and up.userid = $1
        """
        fund_holding = await main_db_instance.fetch_rows(query, user_id)
        # count = fund_holding

        logger.info(f'Found holding(s) for user {user_info["email"]}')
        return {'holdings': fund_holding}

    except Exception as exp:
        logger.exception(f'Exception while looking up {user_info["email"]} holdings: {exp}')
        return JSONResponse(status_code=500, content="Error looking up users holdings")