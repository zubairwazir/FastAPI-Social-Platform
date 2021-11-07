import logging
from typing import Optional
from models_logic import *
from db_sessions import main_db_instance
from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from cache.apiroute import CachingLayerRoute
from dependencies.validation import validate_token_dependency
from task_scheduler.cpu_bound import CPUBoundTaskScheduler
from utils import schedule_task

logger = logging.getLogger('COMPANY_ROUTER')

router = APIRouter(
    prefix="/company",
    tags=['company'],
    responses={404: {'description': 'Not found'}},
    dependencies=[Depends(validate_token_dependency)]
)

scheduler = CPUBoundTaskScheduler(max_workers=4)


@router.on_event('shutdown')
def shutdown():
    scheduler.shutdown(wait=True)


# add caching layer
router.route_class = CachingLayerRoute


@router.get("/{company_ticker}/carbon-footprint")
async def get_company_footprint_controller(company_ticker: str):
    logger.info(f"Getting {company_ticker} ticker carbon footprint data")
    footprint = await get_carbon_footprint(company_ticker)
    footprint_json = jsonable_encoder(footprint)
    return footprint_json


@router.get("/{company_ticker}/related-companies")
async def get_related_companies_controller(company_ticker: str):
    logger.info(f'Getting related companies of {company_ticker} ticker')
    related_companies = await get_related_companies(company_ticker)
    return related_companies


@router.get("/{company_ticker}/news")
async def get_news_company_controller(company_ticker: str):
    company_news = await get_company_news(company_ticker)
    return company_news


# carbon historical
@router.get("/{company_ticker}/sumhistoriccarbon/{year_range}")
async def get_company_sum_historic_carbon_controller(
        company_ticker: str,
        year_range: int
):
    historic_carbon = await get_sum_historic_carbon(company_ticker, year_range)
    return historic_carbon

# carbon temp conversation
@router.get("/{company_ticker}/temperatureconversion/{year_range}")
async def get_company_temperature_conversion_controller(
        company_ticker: str,
        year_range: int
):
    temperature_conversion = await get_temperature_conversion(company_ticker, year_range)
    return temperature_conversion

# company industry sum - take company and return time series of average within it's industry
@router.get("/{company_ticker}/industry-sum")
async def get_company_industry_sum_controller(company_ticker: str):
    industry_sum = await get_industry_sum(company_ticker)
    return industry_sum


# company carbon efficiently
@router.get("/{company_ticker}/EmissionsEfficiency")
async def get_company_emissions_efficiency_controller(company_ticker: str):
    emissions_efficiency = await get_emissions_efficiency(company_ticker)
    return emissions_efficiency


# company carbon efficiently
@router.get("/{company_ticker}/carbontax")
@router.get("/{company_ticker}/carbontax/{financialColumn}")
async def get_company_carbon_tax_controller(
        company_ticker: str,
        financial_column: Optional[str] = 'netincome'
):
    carbon_tax = await get_carbon_tax(company_ticker, financial_column)
    return carbon_tax


# company industry averages - take company and return time series of average within it's industry
@router.get("/{company_ticker}/industry-average")
async def get_company_industry_average_controller(
        company_ticker: str
):
    industry_average = await get_company_industry_average(company_ticker)
    return jsonable_encoder(industry_average)


@router.get("/{company_ticker}/industry-temp-impact/{year}")
async def get_company_industry_temp_impact_controller(
        company_ticker: str,
        year: int
):
    ind_temp_impact = await get_industry_temp_impact(company_ticker, year)
    return ind_temp_impact


# company carbon over net income
@router.get("/{company_ticker}/netincome-carbon")
async def get_company_net_income_over_carbon_controller(
        company_ticker: str
):
    query_statement = """
    select c.carbon, fisy.netincome,
    round( (CAST (fisy.netincome AS float) / CAST (c.carbon AS float))::DECIMAL, 2) as NetIncomeOverCarbon,
    c.year

    from "company"."financials_Income_Statement_yearly" fisy

    INNER JOIN company.carbon c ON upper(c.ticker) = upper(fisy.ticker)
        and c.year =  EXTRACT(YEAR FROM CAST(fisy.date AS DATE))

    where upper(c.ticker) = $1

    order by "date" asc
    """

    result_data = await main_db_instance.fetch_rows(query_statement, company_ticker.upper())
    return jsonable_encoder(result_data)


# 15Rock model score
@router.get("/{company_ticker}/15rock-globalscore")
async def get_company_15rock_global_score_controller(
        company_ticker: str
):
    query_statement = """
    with data as
    (select g.ticker, g.industry , g.countryname, g.exchange, c.carbon, c.year,
    ROW_NUMBER() OVER (
    PARTITION BY g.industry,c."year" --, g.exchange
    ORDER BY c.carbon ASC
    ) AS groupingNumRank,
    AVG(c.carbon) over (
    PARTITION BY g.industry, c."year" --, g.exchange
    ) as industavg,
    --sql test
    max(c.carbon) over (
    PARTITION BY g.industry, c."year" --, g.exchange
    ) as industMAX,
    min(c.carbon) over (
    PARTITION BY g.industry, c."year" --, g.exchange
    ) as industMIN
    from company."General" g
    INNER JOIN company.carbon c ON upper(c.ticker) =g.ticker
    --and c.year = '2019'
    )
    select ticker, industry , countryname, exchange, year, carbon, industavg, groupingNumRank, industMAX, industMIN, (data.industavg/data.carbon) as GlobalModelScore, ( 100 - ROUND(data.carbon * 100.0 / data.industMAX, 2)  )AS GlobalModelPercent
    from data
    -- comment below to get entire sector
    where upper(data.ticker) = $1

    """

    result_data = await main_db_instance.fetch_rows(query_statement, company_ticker.upper())
    return jsonable_encoder(result_data)


# carbon breakdown
@router.get("/{company_ticker}/co2_breakdown")
async def get_company_15rock_co2_breakdown_controller(
        company_ticker: str
):
    query_statement = """

    select c.carbon,
    --round( (CAST (fisy.netincome AS float) / CAST (c.carbon AS float))::DECIMAL, 2) as NetIncomeOverCarbon,
    -- high level predictions
    round( (CAST (c.carbon AS float) * .36 )::DECIMAL, 2) as StationaryCombustion,
    round( (CAST (c.carbon AS float) * .1188 )::DECIMAL, 2) as StationaryCombustionFromPetroleumProducts,
    round( (CAST (c.carbon AS float) * .0324 )::DECIMAL, 2) as StationaryCombustionFromCoal,
    round( (CAST (c.carbon AS float) * 0.1476 )::DECIMAL, 2) as StationaryCombustionFromNaturalGas,
    round( (CAST (c.carbon AS float) * 0.35 )::DECIMAL, 2) as MobileCombustion,
    round( (CAST (c.carbon AS float) * 0.315 )::DECIMAL, 2) as MobileCombustionFromPetroleumProducts,
    -- Petroleum Products
    round( (CAST (c.carbon AS float) * .1188 * .9999497 )::DECIMAL, 2) as CO2FromPetroleumStationary,
    round( (CAST (c.carbon AS float) * .1188 * .0000419 )::DECIMAL, 2) as MethaneFromPetroleumStationary,
    round( (CAST (c.carbon AS float) * .1188 * .0000084 )::DECIMAL, 2) as NitroFromPetroleumStationary,
    -- Coal products  by
    round( (CAST (c.carbon AS float) * .0324 * .9998717 )::DECIMAL, 2) as CO2FromCoalStationary,
    round( (CAST (c.carbon AS float) * .0324 * .000112 )::DECIMAL, 2) as MethaneFromCoalStationary,
    round( (CAST (c.carbon AS float) * .0324 * .0000163 )::DECIMAL, 2) as NitroFromCoalStationary,
    -- Natural Gas products  by
    round( (CAST (c.carbon AS float) * .1476 * .9999793 )::DECIMAL, 2) as CO2FromNaturalGasStationary,
    round( (CAST (c.carbon AS float) * .1476 * .0000188 )::DECIMAL, 2) as MethaneFromNaturalGasStationary,
    round( (CAST (c.carbon AS float) * .1476 * .0000019 )::DECIMAL, 2) as NitroFromNaturalGasStationary,
    -- Mobile combustion products  by
    round( (CAST (c.carbon AS float) * .315 * .9998707 )::DECIMAL, 2) as CO2FromPetroleumMobile,
    round( (CAST (c.carbon AS float) * .315 * .00011 )::DECIMAL, 2) as MethaneFromPetroleumMobile,
    round( (CAST (c.carbon AS float) * .315 * .00000193 )::DECIMAL, 2) as NitroFromPetroleumMobile,

    c.year



    from "company"."financials_Income_Statement_yearly" fisy

    INNER JOIN company.carbon c ON upper(c.ticker) = upper(fisy.ticker)
        and c.year =  EXTRACT(YEAR FROM CAST(fisy.date AS DATE))

    where upper(c.ticker) = $1

    order by "date" asc

    """

    result_data = await main_db_instance.fetch_rows(query_statement, company_ticker.upper())
    return jsonable_encoder(result_data)


# carbon  Equivalencies Calculator
@router.get("/{company_ticker}/equivalencies_calculator")
async def get_company_15rock_equivalencies_calculator_controller(
        company_ticker: str
):
    query_statement = """

    select c.carbon,
    round( (CAST (fisy.netincome AS float) / CAST (c.carbon AS float))::DECIMAL, 2) as NetIncomeOverCarbon,
    round( (CAST (c.carbon AS float) * 121643 )::DECIMAL, 2) as NumSmartPhones,
    round( (CAST (c.carbon AS float) * 113 )::DECIMAL, 2) as GasolineConsumedGallons,
    round( (CAST (c.carbon AS float) * 1105 )::DECIMAL, 2) as poundsCoalBurned,
    round( (CAST (c.carbon AS float) * 0.182 )::DECIMAL, 2) as homeElectricityOneYear,
    round( (CAST (c.carbon AS float) * 2513 )::DECIMAL, 2) as milesDrivenByAverageCar,
    round( (CAST (c.carbon AS float) * 0.217 )::DECIMAL, 2) as vhiclesDrivenOneYear,
    round( (CAST (c.carbon AS float) * 98.2 )::DECIMAL, 2) as DieselConsumedInGallons,
    round( (CAST (c.carbon AS float) * 0.12 )::DECIMAL, 2) as HomeEnergyUseForYear,
    -- Greenhouse gas avoided by
    round( (CAST (c.carbon AS float) * 37.9 )::DECIMAL, 2) as lampsSwitchedTOLEDs,
    round( (CAST (c.carbon AS float) * 0.34 )::DECIMAL, 2) as WasteRecycledInsteadLandfilled,
    round( (CAST (c.carbon AS float) * 42.5 )::DECIMAL, 2) as TrashBagsRecycledInsteadOfLandfilled,
    round( (CAST (c.carbon AS float) * 0.0002 )::DECIMAL, 2) as windTurbinesRunningForYear,
    -- Carbon Sequestered by
    round( (CAST (c.carbon AS float) * 16.5 )::DECIMAL, 2) as TreesSeedlingsGrownforDecade,
    round( (CAST (c.carbon AS float) * 1.2 )::DECIMAL, 2) as AcresOfForstOneYear,

    -- Carbon food
    round( (CAST (c.carbon AS float) * 47.61 )::DECIMAL, 2) as CheeseProducedInKG,
    round( (CAST (c.carbon AS float) * 333.33 )::DECIMAL, 2) as MilkProducedInKG,
    round( (CAST (c.carbon AS float) * 250 )::DECIMAL, 2) as RiceProducedInKG,
    round( (CAST (c.carbon AS float) * 166.67 )::DECIMAL, 2) as PoultryMeatProducedInKG,  

    c.year



    from "company"."financials_Income_Statement_yearly" fisy

    INNER JOIN company.carbon c ON upper(c.ticker) = upper(fisy.ticker)
        and c.year =  EXTRACT(YEAR FROM CAST(fisy.date AS DATE))

    where upper(c.ticker) = $1

    order by "date" asc

    """

    result_data = await main_db_instance.fetch_rows(query_statement, company_ticker.upper())
    return jsonable_encoder(result_data)


# historical prices
@router.get("/{company_ticker}/historicalPrices/{limitdate}")
@router.get("/{company_ticker}/historicalPrices")
async def get_company_historical_prices_controller(
        company_ticker: str,
        limit_date: Optional[int] = 4000
):
    historical_prices = await get_historical_prices(company_ticker, limit_date)
    return historical_prices


@router.get("/{company_ticker}/valuation")
async def get_company_valuation_controller(
        company_ticker: str
):
    company_valuation = await get_company_valuation(company_ticker)
    return company_valuation


@router.get("/{company_ticker}/carbongrowthrate")
async def get_company_carbon_growth_rate_controller(
        company_ticker: str
):
    carbon_growth_rate = await get_carbon_growth_rate(company_ticker)
    return carbon_growth_rate


@router.get("/{company_ticker}/carboncapture")
async def get_company_carbon_capture_controller(
        company_ticker: str
):
    carbon_capture = await get_carbon_capture(company_ticker)
    return carbon_capture


@router.get("/{company_ticker}/productionefficency")
async def get_company_production_efficiency_controller(
        company_ticker: str
):
    production_efficiency = await get_production_efficiency(company_ticker)
    return production_efficiency


# company carbon alpha
@router.get("/{company_ticker}/carbonAlpha/{pct_carbon}")
async def get_company_carbon_alpha_controller(
        company_ticker: str,
        pct_carbon: int
):
    carbon_alpha = await get_carbon_alpha(company_ticker, pct_carbon)
    return carbon_alpha


# getCarbonTransitionRisk
@router.get("/{company_ticker}/CarbonTransitonRisk")
@router.get("/{company_ticker}/CarbonTransitonRisk/{pct_carbon}")
async def get_company_carbon_transition_risk_controller(
        company_ticker: str,
        pct_carbon: Optional[int] = 3,
):
    carbon_trans_risk = await get_carbon_transition_risk(company_ticker, pct_carbon)
    return carbon_trans_risk


# get cogs
@router.get("/{company_ticker}/cogs")
async def get_company_cogs_controller(
        company_ticker: str,
):
    logger.info(f'Getting COGS for {company_ticker}')
    cogs = await get_cogs(company_ticker)
    return cogs


# getCarbonBudget
@router.get("/{company_ticker}/carbonbudget")
async def get_company_carbon_budget_controller(company_ticker: str):
    logging.info(f"Calculating carbon budget for {company_ticker}")

    carbon_budget = await schedule_task(scheduler, get_carbon_budget, company_ticker)
    return jsonable_encoder(carbon_budget)


# financials
@router.get("/{company_ticker}/financials")
async def get_company_financials_controller(
        company_ticker: str
):
    logger.info(f'Getting financials for {company_ticker}')
    financials = await get_company_financials(company_ticker)
    return financials


# general info on company
@router.get("/{company_ticker}/info")
@router.get("/{company_ticker}")
async def get_company_summary_controller(
        company_ticker: str,
):
    company_info = await get_company_info(company_ticker)
    return company_info
