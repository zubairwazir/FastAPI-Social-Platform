import logging
import traceback
import uvicorn
from fastapi import FastAPI
from config import config
from fastapi import Response, Request
from typing import List, Optional
from sql_queries import get_tables_string
from domain import ModelItem
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from db_sessions import main_db_instance
from fastapi.middleware.cors import CORSMiddleware
from cache.apiroute import CachingLayerRoute, _redis
from routers import fund, company, portfolio, country, user, userinfo
from http_client import http_client
from models_logic import \
    get_fund_holdings_weights, \
    get_fund_data, search_company, \
    get_company_chart_endpoints,\
    get_portfolio_chart_endpoints


app = FastAPI()
origins = [
    "https://www.15rock.com",
    "https://15rock.com",
    "api.15rock.com",
    "https://api.15rock.com",
    "http://localhost",
    "https://localhost",
    "https://localhost:8080",
    "https://15rock.webflow.io",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# adding middleware caching Try to move caching AFTER CORS setup
app.router.route_class = CachingLayerRoute

# include routers
app.include_router(fund.router)
app.include_router(company.router)
app.include_router(country.router)
app.include_router(portfolio.router)
app.include_router(user.router)
app.include_router(userinfo.router)


logging.basicConfig(level=logging.INFO)


# catch all exceptions
async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception:
        # you probably want some kind of logging here
        logging.exception(f'Unhandled exception in the controller: {traceback.format_exc()}')
        return Response("Internal server error", status_code=500)


app.middleware('http')(catch_exceptions_middleware)


@app.on_event("startup")
async def startup():
    await main_db_instance.connect()
    # await log_db_instance.connect()
    http_client.start()
    app.state.cache = _redis


@app.on_event("shutdown")
async def shutdown():
    await main_db_instance.disconnect()
    await _redis.close()
    await http_client.stop()


# this will get the tickers of a fund
@app.get("/fund/{fund_ticker}/holdings")
@app.get("/fund/{fund_ticker}/holdings/{imputation}")
async def get_fund_summary(
        fund_ticker: str,
        imputation: Optional[str] = 'None',
):
    fund_holdings_weight = await get_fund_holdings_weights(fund_ticker, imputation)
    return jsonable_encoder(fund_holdings_weight)


@app.get("/fund/{fund_ticker}/data")
async def get_investment_fund_data(
        fund_ticker: str,
):
    fund_data = await get_fund_data(fund_ticker)
    return jsonable_encoder(fund_data)


@app.get("/industry/{sector}")
async def get_industry_summary(
        sector: str,
):
    query_statement = 'select * from "company"."companyData" where "companyData"."Sector" = $1'

    industry_split = sector.split("-")
    formatted_sector = ""

    for item_pos in range(0, len(industry_split)):
        formatted_field = industry_split[item_pos].capitalize()
        formatted_sector += formatted_field

        if item_pos < len(industry_split) - 1:
            formatted_sector += " "

    result_data = await main_db_instance.fetch_rows(query_statement, formatted_sector)
    return jsonable_encoder(result_data)


@app.get("/operations/tickers")
async def get_all_tickers():
    query_statement = """
    select ticker, name, code
    from "company"."General" g
    where g."type" = 'Common Stock'
    """

    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


@app.get("/search/{search_name}")
async def get_search_names(
        search_name: str,
):
    results = await search_company(search_name)
    return jsonable_encoder(results)


@app.get("/website/company/endpoints")
async def get_all_endpoints_controller():
    endpoints = await get_company_chart_endpoints()
    return jsonable_encoder(endpoints)


@app.get("/website/portfolio/endpoints")
async def get_all_port_endpoints_controller():
    endpoints = await get_portfolio_chart_endpoints()
    return jsonable_encoder(endpoints)



@app.get("/")
async def root():
    return JSONResponse({
        "success": True,
        "message": "Welcome to the 15Rock API. Please go to 15Rock.com for documentation and get your token."
    }, status_code=200)


@app.delete("/cache/delete")
async def cache_delete(patterns: List[str]):
    deleted_keys = []
    logging.info(f'Deleting cache keys for patterns: {patterns}')

    for pattern in patterns:
        keys = await app.state.cache.delete(pattern)
        deleted_keys.extend(keys)

    return deleted_keys


@app.get('anonymous-token')
async def anonymous_token():
    rows = await main_db_instance.fetch_rows("select token from company.user_profile were email='anonymous@15rock.com'")

    if len(rows) != 0:
        return jsonable_encoder(rows[0])
    else:
        return {"success": False, "message": "Anonymous user not in db."}


@app.post("/model")
async def get_data(
        item: ModelItem
):
    for field in item.group_fields:
        if field not in item.show_fields_1:
            item.show_fields_1.append(field)

    t_name_1: str = "companyData"
    t_name_2: str = "carbon"
    col_names_1 = get_tables_string(item.show_fields_1, t_name_1)
    col_names_2 = get_tables_string(item.show_fields_2, t_name_2)

    select_clause = 'SELECT {col_names_1}, {col_names_2} '.format(
        col_names_1=col_names_1,
        col_names_2=col_names_2
    )
    from_clause = 'FROM "company"."{t_name_1}", "company"."{t_name_2}" '.format(
        t_name_1=t_name_1,
        t_name_2=t_name_2
    )
    where_clause = 'WHERE "{t_name_1}"."ticker" = "{t_name_2}"."ticker" and "{t_name_2}"."year" = {year} '.format(
        t_name_1=t_name_1,
        t_name_2=t_name_2,
        year=item.year
    )
    order_by_clause = 'ORDER BY {score_field} DESC'.format(
        score_field=item.score_field)

    get_data_query = select_clause + from_clause + where_clause + order_by_clause

    result_data = await main_db_instance.fetch_rows(get_data_query)
    result_data_json = jsonable_encoder(result_data)

    group_by_data = {}
    flattened_data = []

    def group_field_to_str(elem, pos_data):
        return "{elem}:{pos_data}".format(elem=elem, pos_data=pos_data)

    # grouping by provided group fields
    for i in range(1, len(result_data_json)):
        gprstr = ",".join([group_field_to_str(
            elem, result_data_json[i][elem]) for elem in item.group_fields])
        if gprstr in group_by_data:
            group_by_data[gprstr].append(result_data_json[i])
        else:
            group_by_data[gprstr] = [result_data_json[i]]

    for key in group_by_data:
        len_1 = len(group_by_data[key])
        sum = 0.0
        for j in range(0, len_1):
            ele = group_by_data[key][j]
            sum += ele[item.score_field]

        for j in range(0, len_1):
            ele = group_by_data[key][j]
            ele["Score"] = round(float(ele[item.score_field] / sum * len_1), 2)
            ele["Rank"] = j + 1
            flattened_data.append(ele)

    return JSONResponse(flattened_data)


if __name__ == "__main__":
    # app.run()
    uvicorn.run("main:app", host="localhost", port=config.port, reload=False)
