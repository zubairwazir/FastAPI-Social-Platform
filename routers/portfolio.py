import logging
import pandas as pd
from fastapi import APIRouter, Depends, Body
from fastapi.encoders import jsonable_encoder
from cache.apiroute import CachingLayerRoute
from dependencies.validation import validate_token_dependency
from models_logic import (
    get_cogs,
    get_carbon_footprint,
    get_historical_prices,
    get_portfolio_scores
)
from db_sessions import main_db_instance
from domain import PortfolioItems
from functools import wraps

logger = logging.getLogger('PORTFOLIO_ROUTER')

router = APIRouter(
    prefix='/portfolio',
    tags=['portfolio'],
    responses={404: {'description': 'Not found'}},
    dependencies=[Depends(validate_token_dependency)]
)

router.route_class = CachingLayerRoute

# carbon footprint
@router.post("/analytics/carbon-footprint")
async def get_portfolio_carbon_footprint_controller(
        tickers: list = Body(...),
        func: str = Body(...),
):
    portfolioDF = pd.DataFrame()
    jsonResults = await get_carbon_footprint(tickers)
    elementDF = pd.DataFrame(jsonResults)
    portfolioDF = portfolioDF.append(elementDF, ignore_index=True)

    # numerical columns
    portfolioDF = portfolioDF.select_dtypes(['number'])
    gr = portfolioDF.groupby('year').agg(func)
    gr.reset_index(inplace=True)
    res = gr.to_json(orient='records')
    res = eval(res)  # TODO: eval is dangerous, are we sure we want to use it
    return res


# portfolioCOGS
@router.post("/analytics/cogs")
async def get_portfolio_cogs_footprint_controller(
        tickers: list = Body(...),
        func: str = Body(...),
):
    portfolioDF = pd.DataFrame()
    jsonResults = await get_cogs(tickers)
    elementDF = pd.DataFrame(jsonResults)
    portfolioDF = portfolioDF.append(elementDF, ignore_index=True)

    # numerical columns
    portfolioDF = portfolioDF.select_dtypes(['number'])
    gr = portfolioDF.groupby('year').agg(func)
    gr.reset_index(inplace=True)
    res = gr.to_json(orient='records')
    res = eval(res)
    return res


# historical prices
@router.post("/analytics/historicalprices")
async def get_portfolio_historical_prices_controller(
        tickers: list = Body(..., embed=True)
):
    hist_prices = await get_historical_prices(tickers)
    return hist_prices

# portfolio score prices
@router.post("/analytics/score")
async def get_portfolio_historical_scores_controller(
        tickers: list = Body(...),
        func: str = Body(...),
):
    logger.info(f"tickers at endpoint are {tickers}")
    port_scores = await get_portfolio_scores(tickers, func)
    return port_scores

# TODO:  work in progress
@router.post("/analytics/sortino")
async def get_portfolio_carbon_footprint_controller(
        tickers: list = Body(...),
        func: str = Body(...),
):
    portfolioDF = pd.DataFrame()
    for element in tickers:
        # print(element)
        jsonResults = jsonable_encoder(await get_carbon_footprint(tickers=element.upper()))['body']
        # print(jsonResults)
        elementDF = pd.read_json(jsonResults)
        # portfolioDF = pd.concat([elementDF, portfolioDF])
        portfolioDF = portfolioDF.append(elementDF, ignore_index=True)
        # print(portfolioDF)

    # numerical columns
    portfolioDF = portfolioDF.select_dtypes(['number'])
    gr = portfolioDF.groupby('year').agg(func)
    gr.reset_index(inplace=True)
    res = gr.to_json(orient='records')
    res = eval(res)
    return res


# portfolio carbon individual for each company/year
@router.post("/carbon-footprint")
async def get_portfolio_carbon_controller(
        items: PortfolioItems
):
    query_statement = '''
    select ticker, year, carbon, provided
    from "company"."carbon" c
    where upper(c.ticker) =  ANY($1)
    order by ticker, year ASC;

    '''

    result_data = await main_db_instance.fetch_rows(query_statement, [element.upper() for element in items.tickers])
    return jsonable_encoder(result_data)


async def run_portfolio(items, portfolio_type):
    query_statement = f'''
    select year, AVG({portfolio_type}) AS AVG_{portfolio_type}
    from "company"."{portfolio_type}" c
    where upper(c.ticker)  = ANY($1)
    GROUP BY year
    order by year asc
    '''

    result_data = await main_db_instance.fetch_rows(query_statement, [element.upper() for element in items.tickers])
    return jsonable_encoder(result_data)


def portfolio_average(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        response = await run_portfolio(*args, **kwargs)
        return response

    return wrapper


# portfolio carbon average for portfolio
@router.post("/carbon-averages")
@portfolio_average
async def get_portfolio_carbon_average_controller(
        items: PortfolioItems,
        portfolio_type: str = "carbon",
):
    pass


@router.post("")
async def get_portfolio_summaries_controller(
        items: PortfolioItems
):
    query_statement = 'select * from "company"."General" where ticker = ANY($1)'

    result_data = await main_db_instance.fetch_rows(query_statement, [element.upper() for element in items.tickers])
    return jsonable_encoder(result_data)
