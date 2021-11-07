from db_sessions import main_db_instance
import pandas as pd
import numpy as np
from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm
from datetime import datetime
import py15rock
from concurrent.futures import ThreadPoolExecutor
from config import config
from fastapi.encoders import jsonable_encoder
from utils import convert_tickers
import logging

logger = logging.getLogger('COMPANY_ROUTER')

# 15rock api code
py15rock.get.config.api_key = config.rock_api_key
py15rock.get.config.api_endpoint = config.rock_url


async def get_carbon_footprint(tickers):
    processed_tickers = convert_tickers(tickers)
    query_statement = f"""
    select ticker, year, carbon, provided 
    from company.carbon 
    where upper(ticker) in {processed_tickers} 
    order by year ASC
    """

    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


async def get_sum_historic_carbon(tickers, year: int):
    processed_tickers = convert_tickers(tickers)

    query_statement = f"""
        WITH T as (
        select carbon
        from company.carbon cb
        where upper(ticker) in {processed_tickers}
        order by year desc
        limit {year}
        ) 
        select SUM(carbon) as totalcarbon from T 
    
    """

    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


async def get_temperature_conversion(tickers, year: int):
    processed_tickers = convert_tickers(tickers)

    query_statement = f"""
        WITH T as (
        select carbon
        from company.carbon cb
        where upper(ticker) in {processed_tickers}
        order by year desc
        limit {year}
        ) 
        select SUM(carbon) as totalcarbon, 
        round(SUM(carbon)/1000000000000::numeric, 60) as CarbonInTeratonnes, 
        round(SUM(carbon)/1000000000000::numeric * 1.6, 60) as ChangeTemperatureMean, 
        round(SUM(carbon)/1000000000000::numeric * 1, 60) as ChangeTemperature5thPercentile, 
        round(SUM(carbon)/1000000000000::numeric * 2.1, 60) as ChangeTemperature95thPercentile	
        from T 
    """
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


async def get_industry_sum(tickers):
    processed_tickers = convert_tickers(tickers)
    
    query_statement = f"""
    select
    year,
    cast(SUM(carbon) as NUMERIC) AS SUM_carbon,
    cast(MAX(carbon) FILTER (WHERE upper(ticker) in {processed_tickers} ) as NUMERIC) AS company_carbon
    from "company"."carbon" c
    where lower(c.ticker) in (
            select lower(g4.ticker)
            from "company"."General" g4
            where industry = (
                    select industry
                    from "company"."General" g3
                    where upper(g3.ticker) in {processed_tickers}
                )
        )
    GROUP BY c.year
    having MAX(carbon) FILTER (WHERE upper(ticker) in {processed_tickers}) is not null
    order by year ASC;
    """
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


async def get_industry_temp_impact(tickers, year):
    processed_tickers = convert_tickers(tickers)
    query_statement = f"""
    WITH T as (
    select cast(SUM(carbon) as NUMERIC) AS industry_carbon
    from "company"."carbon" c
    where lower(c.ticker) in (
            select lower(g4.ticker)
            from "company"."General" g4
            where industry = (
                    select industry
                    from "company"."General" g3
                    where upper(g3.ticker) in {processed_tickers}
                )
        )
    GROUP BY c.year
    having MAX(carbon) FILTER (WHERE upper(ticker) in {processed_tickers} ) is not null
    order by year desc
    limit {year}
    )
    
        select SUM(industry_carbon) as totalcarbon, 
        round(SUM(industry_carbon)/1000000000000::numeric, 60) as CarbonInTeratonnes, 
        round(SUM(industry_carbon)/1000000000000::numeric * 1.6, 60) as ChangeTemperatureMean, 
        round(SUM(industry_carbon)/1000000000000::numeric * 1, 60) as ChangeTemperature5thPercentile, 
        round(SUM(industry_carbon)/1000000000000::numeric * 2.1, 60) as ChangeTemperature95thPercentile	
        from T 

    """
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


async def get_emissions_efficiency(tickers):
    # TODO this might be a duplicate, confirm and delete
    processed_tickers = convert_tickers(tickers)
    query_statement = f"""
    select c.year, 
    --sum(fisy.totalrevenue) as industryTotalRevenue,
    --sum(fisy.grossprofit) as indsutryGrossProfit,
    ln(sum(fisy.totalrevenue) - sum(fisy.grossprofit)) as industryCOGS,
    sum(c.carbon) as industryCarbon,
    --sum(revenue) filter (where upper(company) in {processed_tickers} ) / sum(revenue),
    ln(sum(fisy.totalrevenue) filter (where upper(fisy.ticker) in {processed_tickers} ) - sum(fisy.grossprofit) filter (where upper(fisy.ticker) in {processed_tickers})) as companyCOGS,
    (avg(c.carbon) filter (where upper(fisy.ticker) in {processed_tickers} )) / ln(sum(fisy.totalrevenue) filter (where upper(fisy.ticker) in {processed_tickers}) - sum(fisy.grossprofit) filter (where upper(fisy.ticker) in {processed_tickers})) as CarbonoverCOGS,
    avg(c.carbon) / ln(sum(fisy.totalrevenue) filter (where upper(fisy.ticker) in {processed_tickers} ) - sum(fisy.grossprofit) filter (where upper(fisy.ticker) in {processed_tickers} )) / ln(sum(fisy.totalrevenue) filter (where upper(fisy.ticker) in {processed_tickers} ) - sum(fisy.grossprofit) filter (where upper(fisy.ticker) in {processed_tickers} )) as ExcessCarbonOverIndusry
    --(c.carbon/ln(fisy.totalrevenue- fisy.grossprofit)) as emissionsPerCOGS
    from "company"."carbon" c
    join company."financials_Income_Statement_yearly" fisy 
    on upper(fisy.ticker) = upper(c.ticker) 
    where lower(c.ticker) in (
            select lower(g4.ticker)
            from "company"."General" g4
            where industry = (
                    select industry
                    from "company"."General" g3
                    where upper(g3.ticker) in {processed_tickers}
                )
        ) and fisy.grossprofit != fisy.totalrevenue
        and extract(year from fisy.date) = c.year
    GROUP BY c.year
    order by year ASC;

    """
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


# historical prices
async def get_historical_prices(tickers, limitdate):
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(tickers)
    query_statement = f"""
    select * 
    from company."EODprice" e 
    where ticker in {processed_tickers}
    --and e.date > '2021-01-01'
    order by date desc
    limit {limitdate}
    """
    
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


async def get_company_valuation(tickers):
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(tickers)
    query_statement = f"""
    select * 
    from company."Valuation" v 
    where ticker in {processed_tickers}
    """
    
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


# get fund data
async def get_fund_data(fund_ticker):
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(fund_ticker)
    query_statement = f"""
    select * 
    from company.fund_data fd 
    where upper(ticker) in {processed_tickers}
    """
    
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


# getCountryCarbonHistory
async def get_country_carbon_history(tickers):
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(tickers)
    query_statement = f"""
    select country, year, co2emissions
    from company."country_emissions" e 
    where REPLACE(upper(country),' ','') in {processed_tickers}
    order by year desc

    """
    
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


# getCountryCarbonHistory
async def get_country_tax(tickers):
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(tickers)
    query_statement = f"""
    select countryname, lower_bound, upper_bound, mean
    from company.tax_regimes tr  
    where REPLACE(upper(countryname),' ','') in {processed_tickers}
    """
    
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


# getWorldCarbonHistory
async def get_world_carbon_history():
    # removing the casting to upper for pricing as items are already upper
    # processed_tickers = convertTickers(tickers)
    query_statement = f"""
    select year, sum(co2emissions) as WorldCarbon
    from company.country_emissions ce 
    group by year
    order by year desc
    """
    
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


async def get_cogs(tickers):
    processed_tickers = convert_tickers(tickers)
    query_statement = f"""
    select date_part('year', date) as year, (totalrevenue -  grossprofit) as cogs
    from company."financials_Income_Statement_yearly" fisy 
    where upper(ticker) in {processed_tickers}
    order by date_part('year', date) desc 
    """
    
    result_data = await main_db_instance.fetch_rows(query_statement)
    
    # if gross profit and total revenue are the same then we can use operating income
    if result_data[0]['cogs'] <= 0:
        query_statement = f"""
        select date_part('year', date) as year, (totalrevenue -  operatingincome) as cogs
        from company."financials_Income_Statement_yearly" fisy 
        where upper(ticker) in {processed_tickers}
        --group by date_part('year', date)
        order by date_part('year', date) desc 
        """
        result_data = await main_db_instance.fetch_rows(query_statement)

    return jsonable_encoder(result_data)


async def get_carbon_growth_rate(tickers):
    processed_tickers = convert_tickers(tickers)
    processed_tickers = processed_tickers[0]

    # get company carbon
    company_carbon = py15rock.get.companyCarbon(processed_tickers)
    # company_carbon = await getCarbonFootprint(processed_tickers)
    company_carbon = pd.DataFrame(company_carbon)
    company_carbon['carbonGrowth'] = company_carbon['carbon'].pct_change()
    # drop nan values
    company_carbon = company_carbon[company_carbon['carbonGrowth'].notna()]

    result_data = {"carbon_growth": company_carbon['carbonGrowth'].mean()}
    return jsonable_encoder(result_data)


async def get_company_industry_average(tickers):
    processed_tickers = convert_tickers(tickers)
    processed_tickers = processed_tickers[0]

    query_statement = f"""
    select
    year,
    cast(AVG(carbon) as NUMERIC) AS AVG_carbon,
    cast(MAX(carbon) FILTER (WHERE upper(ticker) = '{processed_tickers}' ) as NUMERIC) AS company_carbon
    from "company"."carbon" c
    where upper(c.ticker) in (
            select upper(g4.ticker)
            from "company"."General" g4
            where industry = (
                    select industry
                    from "company"."General" g3
                    where upper(g3.ticker) = '{processed_tickers}'
                )
        )
    GROUP BY c.year
    having MAX(carbon) FILTER (WHERE upper(ticker) = '{processed_tickers}') is not null
    order by year ASC;

    """

    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


async def search_company(search_name: str):
    search_name = search_name.upper()

    query_statement = f"""
        SELECT g.ticker, g.name, g.isin, g.cusip, g.cik, 
        --g.ticker as value
        concat(g.name, ' | ', g.ticker ) as value
        FROM "company"."General" g
        WHERE upper(g.ticker) like '%{search_name}%' 
        OR upper(g.name) like '%{search_name}%'
        OR upper(g.isin) like '%{search_name}%'
        OR upper(g.cusip) like '%{search_name}%'
        OR upper(g.cik) like '%{search_name}%'
        AND g.type = 'Common Stock'
        order by g.ticker
        LIMIT 20;
    
    """

    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


# getProductionEfficiency
async def get_production_efficiency(tickers):
    processed_tickers = convert_tickers(tickers)
    processed_tickers = processed_tickers[0]
    # print("ticker is ", processed_tickers)

    # get carbon and industry average
    # - Gross Profit for each company in the industry
    companyCOGS = py15rock.get.companyCOGS(processed_tickers)
    # - Carbon Emissions for each company
    companyCarbon = py15rock.get.companyCarbon(processed_tickers)
    # - Respective industry's average carbon
    companyIndustry = py15rock.get.companyIndustryAverage(processed_tickers)

    # join data
    df = companyCOGS.merge(companyCarbon, on='year', how='right')
    df = df.merge(companyIndustry, on='year', how='right')

    # calcs
    # log cogs
    df['log_cogs'] = np.log1p(df['cogs'].pct_change())
    df['companyEmissionsPerCOGS'] = df['carbon'] / df['log_cogs']
    df['industryEmissionsPerCOGS'] = df['avg_carbon'] / df['log_cogs']
    df['excessCarbonOverIndustryNorm'] = df['industryEmissionsPerCOGS'] - df['companyEmissionsPerCOGS']
    df = df[df['companyEmissionsPerCOGS'].notna()]

    result_data = df.to_dict('records')
    return jsonable_encoder(result_data)


async def get_carbon_capture(tickers):
    processed_tickers = convert_tickers(tickers)
    processed_tickers = processed_tickers[0]

    # get carbon and industry average
    companyIndustry = py15rock.get.companyIndustryAverage(processed_tickers)
    print('company industry ', companyIndustry)
    companyIndustry = companyIndustry.sort_values(by='year', ascending=True)  # sort year
    companyIndustry['company_carbon_chg_from_prev_year'] = companyIndustry[
        'company_carbon'].diff()  # difference in years
    companyIndustry['industry_carbon_chg_from_prev_year'] = companyIndustry['avg_carbon'].diff()  # difference in years
    postiveYears = companyIndustry[companyIndustry['company_carbon_chg_from_prev_year'] >= 0]
    companyIndustry['upsideSTD'] = postiveYears['company_carbon_chg_from_prev_year'].std()
    companyIndustry['carbonCapture'] = (companyIndustry['company_carbon_chg_from_prev_year'] - companyIndustry[
        'industry_carbon_chg_from_prev_year']) / companyIndustry['upsideSTD']
    # cast variables to int
    companyIndustry['industry_carbon_chg_from_prev_year'] = pd.to_numeric(
        companyIndustry['industry_carbon_chg_from_prev_year'])
    companyIndustry['upsideSTD'] = pd.to_numeric(companyIndustry['upsideSTD'])
    companyIndustry['carbonCapture'] = pd.to_numeric(companyIndustry['carbonCapture'])
    companyIndustry['avg_carbon'] = pd.to_numeric(companyIndustry['avg_carbon'])
    companyIndustry = companyIndustry[companyIndustry['carbonCapture'].notna()]

    result_data = companyIndustry.to_dict('records')
    return jsonable_encoder(result_data)


async def get_carbon_tax(tickers, financial_column='netincome'):
    processed_tickers = convert_tickers(tickers)
    processed_tickers = processed_tickers[0]

    companyCarbon = py15rock.get.companyCarbon(processed_tickers)
    # get country tax
    # TODO fix the bug where we are using USA for every review regardless of underlying country
    countryTax = py15rock.get.getCountryTax('usa')
    # get net income
    financials = py15rock.get.getCompany(processed_tickers, 'financials')
    financials = financials[['year', 'ticker', financial_column]]
    # print("financials are : ", financials)
    # join dataset
    financials = financials.merge(companyCarbon, on='year', how='right')
    financials['country_lower_bound'] = countryTax['lower_bound'][0]
    financials['country_upper_bound'] = countryTax['upper_bound'][0]
    financials['country_mean'] = countryTax['mean'][0]
    # calculation
    financials['company_lower_bound'] = (financials['carbon'] * financials['country_lower_bound']) / financials[
        financial_column]
    financials['company_upper_bound'] = (financials['carbon'] * financials['country_upper_bound']) / financials[
        financial_column]
    financials['company_mean'] = (financials['carbon'] * financials['country_mean']) / financials[financial_column]

    # round results
    financials = financials.fillna('')  # test if there is a NAN
    financials = financials.round(5)

    result_data = financials[['year', 'company_lower_bound', 'company_upper_bound', 'company_mean']].to_dict('records')
    return jsonable_encoder(result_data)


def get_carbon_budget(tickers):
    processed_tickers = convert_tickers(tickers)

    # download required data in parrellel
    with ThreadPoolExecutor(max_workers=5) as executor:
        companyCarbon = executor.submit(py15rock.get.companyCarbon, str(processed_tickers[0])).result()
        indexCarbon = executor.submit(py15rock.get.getFundHoldings, "GSPC.INDX").result()
        companyCOGS = executor.submit(py15rock.get.companyCOGS, processed_tickers[0]).result()
        countryCarbon = executor.submit(py15rock.get.getCountryCarbon, "UnitedStates").result()
        worldEmissions = executor.submit(py15rock.get.getCountryCarbon, "world").result()

    # use above dataset to download more data
    indexHoldingsTickers = indexCarbon['ticker'].tolist()
    indexAverageCarbon = py15rock.get.getPortfolioCarbon(indexHoldingsTickers, "sum")
    indexCOGS = py15rock.get.getPortfolioCOGS(indexHoldingsTickers, "sum")

    globalCarbonBudget = 1400000000000
    # get today's year
    currentYear = datetime.now().year
    yearsRemaining = 2116 - 2018  # currentYear
    annualGlobalCarbonBudget = globalCarbonBudget / yearsRemaining

    # get COGS for index
    indexCOGS = indexCOGS.rename(columns={"cogs": "indexcogs"})

    # get COGS for company
    companyCOGS = companyCOGS.rename(columns={"cogs": "companycogs"})

    # merge with other dataframe
    companyCOGS = companyCOGS.merge(indexCOGS, on='year', how='right')

    # get world emissions
    worldEmissions = worldEmissions.rename(columns={"co2emissions": "worldcarbon"})

    # join data
    # join data
    countryCarbon = countryCarbon.merge(worldEmissions[['year', 'worldcarbon']], on='year', how='left')
    indexAverageCarbon = indexAverageCarbon.rename(columns={"carbon": "indexavg"})
    countryCarbon = countryCarbon.merge(indexAverageCarbon[['year', 'indexavg']], on='year', how='right')
    companyCOGS = companyCOGS.merge(countryCarbon, on='year', how='right')
    # companyCOGS = companyCOGS.merge(countryCarbon, on='year', how='right')
    df = companyCarbon.merge(companyCOGS, on='year', how='left')

    # process country dataframe

    df['emissions_pct'] = df['co2emissions'] / df['worldcarbon']
    df['avg_emissions_pct'] = df['emissions_pct'].mean()
    df['currentYearCarbonBudget'] = annualGlobalCarbonBudget * df['avg_emissions_pct']
    df['index_pct'] = df['indexavg'] / df['co2emissions']

    df['avg_index_emissions_pct'] = df['index_pct'].mean()
    df['currentYearIndexCarbonBudget'] = df['currentYearCarbonBudget'] * df['avg_index_emissions_pct']
    df['excessIndexCarbonOverBudgettons'] = df['indexavg'] - df['currentYearIndexCarbonBudget']
    df['excessIndexCarbonOverBudgetpct'] = (
            ((df['indexavg'] - df['currentYearIndexCarbonBudget']) / df['currentYearIndexCarbonBudget']) - 1)
    df['companyCarbonBudget'] = (df['companycogs'] / df['indexcogs']) * df['currentYearIndexCarbonBudget']
    df['companyCarbonBudget'] = df['companyCarbonBudget'].astype('int64')
    df['excessCarbonOverBudgetton'] = df['carbon'] - df['companyCarbonBudget']
    df['excessCarbonOverBudgetpct'] = (((df['carbon'] - df['companyCarbonBudget']) / df['companyCarbonBudget']) - 1)
    df = df.sort_values('year', ascending=False)

    df = df.fillna('')  # test if there is a NAN
    df = df.round(5)

    df = df.to_dict('records')
    return df


async def get_fund_holdings_weights(tickers, imputation='None'):
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(tickers)
    print("fund ticker is ", processed_tickers[0])
    # we can only review one fund at at time
    query_statement = f'''
    select 
    fh.ticker as fund_Name,
    g.ticker,
    fh.sector, 
    fh.industry ,
    max(fh.weight) as weight,
    max(fh.assets_percent) as assets_percent, 
    max(c.carbon) as carbon
    from "company"."fund_Holdings" fh
    LEFT JOIN "company".fund_mapping fm on fh.name=fm.fund_name
    LEFT join "company"."General" g on g.name = fm.mapped_name
    LEFT join "company".carbon c on lower(c.ticker) = lower(g.ticker)
    where upper(fh.ticker) = '{processed_tickers[0]}'
    and c."year" = '2019'
    group by g.ticker, fh.ticker,fh.sector, fh.industry
    '''

    result_data = await main_db_instance.fetch_rows(query_statement)
    if imputation == 'None':
        pass
    elif imputation == 'market':
        query_statement = f"""
        select *
        from company."fund_Holdings" fh 
        where ticker = '{processed_tickers[0]}'
        """
        result_data = await main_db_instance.fetch_rows(query_statement)

    return jsonable_encoder(result_data)


async def get_company_info(tickers):
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(tickers)
    print("we are in get company")
    query_statement = f"""

    select * from "company"."General" where upper(ticker) in {processed_tickers}

    """

    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


async def get_company_financials(tickers):
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(tickers)
    query_statement = f"""

    select *, date_part('year', fisy.date) as year, g.ticker
    from company."General" g 
    join company."financials_Balance_Sheet_yearly" fbsy ON fbsy.ticker = g.ticker
    join company."financials_Cash_Flow_yearly" fcfy ON fcfy.ticker = fbsy.ticker AND fcfy.date = fbsy.date
    join company."financials_Income_Statement_yearly" fisy ON fisy.ticker = fcfy.ticker AND fisy.date = fbsy.date
    --left join company.carbon c on upper(c.ticker) = g.ticker and c.year = date_part('year', fisy.date) -- and c.provided = true 
    left join company."ESGScores" e on e.ticker = g.ticker 
    left join company."Earnings_Annual" ea on ea.ticker = g.ticker and ea.date = cast(fisy.date as text)
    where g.ticker in {processed_tickers}
    order by year desc

    """
    result_data = await main_db_instance.fetch_rows(query_statement)
    print(result_data)
    return jsonable_encoder(result_data)

async def get_portfolio_scores(tickers, func):
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(tickers)
    logger.info(f'User provided us with following tickers {processed_tickers}')
    #let scores
    portfolio_score_df = pd.DataFrame()
    #this should work but it's not
    # for ticker in processed_tickers:
    #     var = await py15rock.get.company15rockScore(ticker)
    #     logger.info(f'we are in in {ticker}')
    #
    #     try:
    #         var = await py15rock.get.company15rockScore(ticker)
    #         # print('Success - ', ticker, " - ", var['globalmodelpercent'])
    #         portfolio_score_df = portfolio_score_df.append(var, ignore_index=True)
    #     except Exception as e:
    #         logger.critical(f"Error - {ticker}  with error of  {e}")

    #this is the big way to do
    for ticker in processed_tickers:
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
        result_data = await main_db_instance.fetch_rows(query_statement, ticker.upper())
        current_portfolio_score_df = pd.DataFrame(jsonable_encoder(result_data))
        current_portfolio_score_df["year"] = pd.to_numeric(current_portfolio_score_df["year"])
        current_portfolio_score_df.reset_index(drop=True, inplace=True)
        portfolio_score_df = portfolio_score_df.append(current_portfolio_score_df) #, ignore_index=True)
    # portfolio_score_df = portfolio_score_df.fillna(0)
    logger.info(f'scores are {portfolio_score_df}')
    portfolio_score_df = portfolio_score_df.groupby('year', as_index=False).agg(func)
    portfolio_score_df = portfolio_score_df[['year', 'carbon', 'globalmodelscore' ]]
    return jsonable_encoder(portfolio_score_df.to_dict('records'))

async def get_company_news(tickers):
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(tickers)
    query_statement = f"""

    select ticker, link, title, "date",	predicted_sentiment,	generated_summary
    from company.news_analytics na 
    where ticker in {processed_tickers}
    order by date desc

    """
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


async def get_company_chart_endpoints():
    # removing the casting to upper for pricing as items are already upper
    # processed_tickers = convertTickers(tickers)
    query_statement = f"""

    select *
    from company.web_companypage_models wcm 
    where portfolio_model is not true 

    """
    result_data = await main_db_instance.fetch_rows(query_statement)
    
    return jsonable_encoder(result_data)


async def get_portfolio_chart_endpoints():
    # removing the casting to upper for pricing as items are already upper
    # processed_tickers = convertTickers(tickers)
    query_statement = f"""

    select *
    from company.web_companypage_models wcm 
    where portfolio_model is  true 

    """
    result_data = await main_db_instance.fetch_rows(query_statement)

    return jsonable_encoder(result_data)


async def get_related_companies(tickers):
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(tickers)

    company_financials = py15rock.get.getCompany(processed_tickers[0], 'financials')
    company_financials = company_financials[
        ['year', 'countryiso', 'name', 'currencycode', 'sector', 'totalassets', 'totalliab', 'grossprofit',
         'totalrevenue', 'netincome']].iloc[0]

    query_statement = f"""
    select distinct on (recommendation, g.name) g.name, g.ticker,
    abs(fbsy.totalassets - {company_financials['totalassets']})* 1+
    abs(fbsy.totalliab - {company_financials['totalliab']})* 5+
    abs(fisy.grossprofit - {company_financials['grossprofit']})* 1+
    abs(fisy.totalrevenue - {company_financials['totalrevenue']})* 10+
    abs(fisy.netincome - {company_financials['netincome']})* 50+
    (g.countryiso <> '{company_financials['countryiso']}')::int* 100000000000000+
    (g.sector <> '{company_financials['sector']}')::int* 100000000000000
    as recommendation,
    case when  g.logourl<>'' then concat('https://eodhistoricaldata.com', g.logourl ) end as logourl
    from company."General" g 
    join company."financials_Balance_Sheet_yearly" fbsy ON fbsy.ticker = g.ticker
    join company."financials_Cash_Flow_yearly" fcfy ON fcfy.ticker = fbsy.ticker AND fcfy.date = fbsy.date
    join company."financials_Income_Statement_yearly" fisy ON fisy.ticker = fcfy.ticker AND fisy.date = fbsy.date
    --left join company.carbon c on upper(c.ticker) = g.ticker and c.year = date_part('year', fisy.date) -- and c.provided = true 
    left join company."ESGScores" e on e.ticker = g.ticker 
    left join company."Earnings_Annual" ea on ea.ticker = g.ticker and ea.date = cast(fisy.date as text)
    where upper(g.name) != upper('{company_financials['name']}')
    order by recommendation, g.name
    limit 10

    """
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)


# async def getCarbonAlpha1(tickers):
#     #TODO not working still
#     #removing the casting to upper for pricing as items are already upper
#     processed_tickers = convertTickers(tickers)[0]
#     query_statement_prices = f"""
#     select *
#     from company."EODprice" e 
#     where e.ticker in ('GSPC.INDX', '{processed_tickers}', 'US10Y.INDX')
#     order by e,ticker 

#     """
#     query_statement_carbon = f"""
#     select * 
#     from company."carbon" c 
#     where ticker = '{processed_tickers}'
#     """

#     query_statement_industryAVG = f"""

#     select
# 	year,
#     cast(AVG(carbon) as NUMERIC) AS AVG_carbon,
#     cast(MAX(carbon) FILTER (WHERE upper(c.ticker) = '{processed_tickers}' ) as NUMERIC) AS company_carbon
#     from company."carbon" c
#     where upper(c.ticker) in (
#             select upper(g4.ticker)
#             from "company"."General" g4
#             where industry = (
#                     select industry
#                     from "company"."General" g3
#                     where upper(g3.ticker) = '{processed_tickers}'
#                 )
#         )
#     GROUP BY c.year
#     having MAX(carbon) FILTER (WHERE upper(c.ticker) = '{processed_tickers}') is not null
#     order by year ASC;

#     """
#     print("processing ", processed_tickers )

#     result_data_price = await main_db_instance.fetch_rows(query_statement_prices)
#     result_data_price = jsonable_encoder(result_data_price)

#     result_data_carbon = await main_db_instance.fetch_rows(query_statement_carbon)
#     result_data_carbon = jsonable_encoder(result_data_carbon)

#     result_data_industry = await main_db_instance.fetch_rows(query_statement_industryAVG)
#     result_data_industry = jsonable_encoder(result_data_industry)

#     industryCarbonDF = pd.DataFrame(result_data_industry)
#     carbonDF = pd.DataFrame(result_data_carbon)
#     pricedataDF = pd.DataFrame(result_data_price)

#     carbonDF['ticker'] = 'equity'
#     pricedataDF['ticker'] = pricedataDF['ticker'].replace(processed_tickers, 'equity')
#     pricedataDF['ticker'] = pricedataDF['ticker'].replace('GSPC.INDX', 'MarketIndex')
#     pricedataDF['ticker'] = pricedataDF['ticker'].replace('US10Y.INDX', 'RiskFreeRate')

#     #shift the values
#     pricedataDF['date'] = pricedataDF['date'].astype('datetime64[ns]') #set date
#     pricedataDFpivot = pricedataDF.pivot(index = 'date', columns = 'ticker', values = 'close') #pivot table
#     pricedataDFpivot['adjustedRF'] = np.log(pricedataDFpivot['RiskFreeRate'] *(1/200))
#     pricedataDFpivot['index_returns'] = np.log(pricedataDFpivot['MarketIndex'] / pricedataDFpivot['MarketIndex'].shift(1))
#     pricedataDFpivot['equity_returns'] = np.log(pricedataDFpivot['equity'] / pricedataDFpivot['equity'].shift(1))
#     pricedataDFpivot['excess_market_returns'] = pricedataDFpivot['index_returns']  - pricedataDFpivot['adjustedRF'] 

#     #remove missing values
#     pricedataDFpivot = pricedataDFpivot.fillna(pricedataDFpivot.rolling(6,min_periods=1).mean()) #replace missing data with 5 day rolling mean. it's 6 because NaN is included
#     pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['equity_returns'].notna()]
#     pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['excess_market_returns'].notna()]

#     percentOfLengh = .5 #.50 #the regression will use this % of the size of the data set
#     windowLen = len(pricedataDFpivot) * percentOfLengh


#     #send the two datacolumns to calc the alpha/beta 
#     results = market_beta(pricedataDFpivot['excess_market_returns'], pricedataDFpivot['equity_returns'], windowLen)
#     #save alpha / beta into their own dataframe
#     results = pd.DataFrame(list(zip(*results)), columns = ['alpha', 'beta']) 
#     #merge those new columns with the existing data
#     pricedataDFpivot[['alpha', 'beta']] = results[['alpha', 'beta']].values
#     #drop all rows with missing alpha(part of the rolling calculation)
#     pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['alpha'].notna()]

#     #look at alpha vs carbon
#     pricedataDFpivot['year'] = pricedataDFpivot.index.year #get the year for each price observation to map the yearly carbon data
#     # pricedataDFpivot['month'] = pricedataDFpivot.index.month
#     pricedataDFpivot['date'] = pricedataDFpivot.index #save date for pricing
#     pricedataDFpivot = pd.merge(pricedataDFpivot, industryCarbonDF, on='year', how='outer') #merge industry and company carbon data
#     pricedataDFpivot['carbon_delta'] = round(pricedataDFpivot['company_carbon'] - pricedataDFpivot['avg_carbon'] ) #get the carbon delta for the second regression
#     pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['equity_returns'].notna()] #drop any column with missing equity - hapens if we have longer carbon than price range
#     pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['carbon_delta'].notna()] # drop any columns with missing carbon

#     #sort 
#     pricedataDFpivot = pricedataDFpivot.sort_values(['year', 'date'], ascending=[True, False]) #sort by year and date
#     pricedataDFpivot = pricedataDFpivot.drop_duplicates(subset=['year'], keep='first') #keep only one year
#     pricedataDFpivot['carbon_delta_log'] = np.log(pricedataDFpivot['carbon_delta'])
#     pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['carbon_delta_log'].notna()]

#     windowLen = len(pricedataDFpivot) -1 #len(pricedataDFpivot) * .9999 #percentOfLengh #recast the % of the dataframe as it's shrunk from the orginal run.
#     # print("length of dataframe is - ", len(pricedataDFpivot))
#     FinalResults = market_beta(pricedataDFpivot['equity_returns'],pricedataDFpivot['carbon_delta_log'],   windowLen) #reuse the regression function
#     #assign the regression's alpha/beta to the orginal dataframe
#     results = pd.DataFrame(list(zip(*FinalResults)), columns = ['carbon_alpha', 'carbon_beta'])
#     pricedataDFpivot[['carbon_alpha', 'carbon_beta']] = results[['carbon_alpha', 'carbon_beta']].values
#     pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['carbon_alpha'].notna()]

#     pricedataDFpivot = pricedataDFpivot.drop(['date'], axis=1)
#     results = pricedataDFpivot.to_dict(('records')) #.to_json()
#     # parsed = json.loads(results)
#     # pricedataDFpivot

#     
#     return results #json.dumps(parsed) 

async def get_carbon_alpha(tickers, pct_carbonChange=1):
    # TODO not working still
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(tickers)[0]
    query_statement_prices = f"""
    select *
    from company."EODprice" e 
    where e.ticker in ('GSPC.INDX', '{processed_tickers}', 'US10Y.INDX')
    order by e,ticker 

    """
    query_statement_carbon = f"""
    select * 
    from company."carbon" c 
    where upper(ticker) = upper('{processed_tickers}')
    """

    query_statement_industryAVG = f"""

    select
	year,
    cast(AVG(carbon) as NUMERIC) AS AVG_carbon,
    cast(MAX(carbon) FILTER (WHERE upper(c.ticker) = '{processed_tickers}' ) as NUMERIC) AS company_carbon
    from company."carbon" c
    where upper(c.ticker) in (
            select upper(g4.ticker)
            from "company"."General" g4
            where industry = (
                    select industry
                    from "company"."General" g3
                    where upper(g3.ticker) = '{processed_tickers}'
                )
        )
    GROUP BY c.year
    having MAX(carbon) FILTER (WHERE upper(c.ticker) = '{processed_tickers}') is not null
    order by year ASC;

    """
    # print("processing ", processed_tickers )

    # control variable
    percentOfLengh = .2  # .50 #the regression will use this % of the size of the data set

    result_data_price = await main_db_instance.fetch_rows(query_statement_prices)
    result_data_price = jsonable_encoder(result_data_price)

    result_data_carbon = await main_db_instance.fetch_rows(query_statement_carbon)
    result_data_carbon = jsonable_encoder(result_data_carbon)

    result_data_industry = await main_db_instance.fetch_rows(query_statement_industryAVG)
    result_data_industry = jsonable_encoder(result_data_industry)

    industryCarbonDF = pd.DataFrame(result_data_industry)
    carbonDF = pd.DataFrame(result_data_carbon)
    pricedataDF = pd.DataFrame(result_data_price)

    carbonDF['ticker'] = 'equity'
    pricedataDF['ticker'] = pricedataDF['ticker'].replace(processed_tickers, 'equity')
    pricedataDF['ticker'] = pricedataDF['ticker'].replace('GSPC.INDX', 'MarketIndex')
    pricedataDF['ticker'] = pricedataDF['ticker'].replace('US10Y.INDX', 'RiskFreeRate')

    # shift the values
    pricedataDF['date'] = pricedataDF['date'].astype('datetime64[ns]')  # set date
    pricedataDFpivot = pricedataDF.pivot(index='date', columns='ticker', values='close')  # pivot table
    pricedataDFpivot['adjustedRF'] = pricedataDFpivot['RiskFreeRate'] * (1 / 200)
    pricedataDFpivot['index_returns'] = np.log(
        pricedataDFpivot['MarketIndex'] / pricedataDFpivot['MarketIndex'].shift(1))
    pricedataDFpivot['equity_returns'] = np.log(pricedataDFpivot['equity'] / pricedataDFpivot['equity'].shift(1))
    pricedataDFpivot['excess_market_returns'] = pricedataDFpivot['index_returns'] - pricedataDFpivot['adjustedRF']
    pricedataDFpivot['excess_returns_alpha'] = pricedataDFpivot['equity_returns'] - pricedataDFpivot[
        'excess_market_returns']

    # remove missing values
    pricedataDFpivot = pricedataDFpivot.fillna(pricedataDFpivot.rolling(6,
                                                                        min_periods=1).mean())  # replace missing data with 5 day rolling mean. it's 6 because NaN is included
    pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['equity_returns'].notna()]
    pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['excess_market_returns'].notna()]

    windowLen = int(len(pricedataDFpivot) * percentOfLengh)
    # add constant column to regress with intercept
    pricedataDFpivot['const'] = 1
    # fit
    model = RollingOLS(endog=pricedataDFpivot['equity_returns'],
                       exog=pricedataDFpivot[['excess_market_returns', 'const']], window=windowLen)
    rres = model.fit()
    pricedataDFpivot['MSE'] = rres.mse_total  # MSE
    pricedataDFpivot['beta'] = rres.params['excess_market_returns']  # beta
    pricedataDFpivot['r2'] = rres.rsquared  # r2
    pricedataDFpivot['alpha'] = rres.params['const']

    # carbon data
    # look at alpha vs carbon
    pricedataDFpivot[
        'year'] = pricedataDFpivot.index.year  # get the year for each price observation to map the yearly carbon data
    # pricedataDFpivot['month'] = pricedataDFpivot.index.month
    pricedataDFpivot['date'] = pricedataDFpivot.index  # save date for pricing
    pricedataDFpivot = pd.merge(pricedataDFpivot, industryCarbonDF, on='year',
                                how='outer')  # merge industry and company carbon data
    pricedataDFpivot['carbon_delta'] = round(pricedataDFpivot['avg_carbon'] - pricedataDFpivot[
        'company_carbon'])  # get the carbon delta for the second regression
    pricedataDFpivot = pricedataDFpivot[pricedataDFpivot[
        'equity_returns'].notna()]  # drop any column with missing equity - hapens if we have longer carbon than price range
    # pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['carbon_delta'].notna()] # drop any columns with missing carbon

    # sort
    pricedataDFpivot = pricedataDFpivot.sort_values(['year', 'date'], ascending=[True, False])  # sort by year and date
    pricedataDFpivot = pricedataDFpivot.drop_duplicates(subset=['year'], keep='first')  # keep only one year
    # pricedataDFpivot['carbon_delta_log'] = np.log(pricedataDFpivot['carbon_delta'])
    pricedataDFpivot['company_carbon_log'] = np.log(pricedataDFpivot['company_carbon'])
    # pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['carbon_delta_log'].notna()]
    pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['alpha'].notna()]
    pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['company_carbon_log'].notna()]

    # second regrssion
    pricedataDFpivot['const'] = 1
    model1 = sm.OLS(endog=pricedataDFpivot['company_carbon_log'], exog=pricedataDFpivot[['alpha', 'const']])
    rres1 = model1.fit()

    pricedataDFpivot['carbon_alpha_MSE'] = rres1.mse_total  # MSE
    pricedataDFpivot['carbon_alpha_beta'] = rres1.params['alpha']  # beta
    pricedataDFpivot['carbon_alpha_r2'] = rres1.rsquared  # r2
    pricedataDFpivot['carbon_alpha_alpha'] = rres1.params['const']
    pricedataDFpivot['alpha_impact'] = pricedataDFpivot['carbon_alpha_r2'] * pricedataDFpivot['carbon_alpha_beta']
    pricedataDFpivot['return_impact_pct'] = pricedataDFpivot['carbon_alpha_r2'] * pricedataDFpivot[
        'carbon_alpha_beta'] * pricedataDFpivot['excess_returns_alpha'] * pct_carbonChange
    pricedataDFpivot['target_carbon'] = pricedataDFpivot['company_carbon'] * (1 - (pct_carbonChange / 100))
    pricedataDFpivot['target_reduction_in_carbon'] = pricedataDFpivot['company_carbon'] * (pct_carbonChange / 100)
    pricedataDFpivot = pricedataDFpivot.rename(columns={'company_carbon': 'current_carbon'})

    pricedataDFpivot = pricedataDFpivot.drop(['date'], axis=1)
    results = pricedataDFpivot[
        ['beta', 'alpha', 'current_carbon', 'target_carbon', 'target_reduction_in_carbon', 'return_impact_pct',
         'alpha_impact']].iloc[-1].to_dict()  # (('records')) #.to_json()

    return results


async def get_carbon_transition_risk(tickers, pct_carbon_change):
    # TODO not working still
    # removing the casting to upper for pricing as items are already upper
    processed_tickers = convert_tickers(tickers)[0]
    query_statement_prices = f"""
    select *
    from company."EODprice" e 
    where e.ticker in ('GSPC.INDX', '{processed_tickers}', 'US10Y.INDX')
    order by e,ticker 
    
    """
    query_statement_carbon = f"""
    select * 
    from company."carbon" c 
    where upper(ticker) = upper('{processed_tickers}')
    """

    query_statement_industryAVG = f"""

    select
	year,
    cast(AVG(carbon) as NUMERIC) AS AVG_carbon,
    cast(MAX(carbon) FILTER (WHERE upper(c.ticker) = '{processed_tickers}' ) as NUMERIC) AS company_carbon
    from company."carbon" c
    where upper(c.ticker) in (
            select upper(g4.ticker)
            from "company"."General" g4
            where industry = (
                    select industry
                    from "company"."General" g3
                    where upper(g3.ticker) = '{processed_tickers}'
                )
        )
    GROUP BY c.year
    having MAX(carbon) FILTER (WHERE upper(c.ticker) = '{processed_tickers}') is not null
    order by year ASC;

    """

    query_statement_financials = f"""

    select * 
    from company."General" g 
    join company."financials_Balance_Sheet_yearly" fbsy ON fbsy.ticker = g.ticker
    join company."financials_Cash_Flow_yearly" fcfy ON fcfy.ticker = fbsy.ticker AND fcfy.date = fbsy.date
    join company."financials_Income_Statement_yearly" fisy ON fisy.ticker = fcfy.ticker AND fisy.date = fbsy.date
    left join company.carbon c on upper(c.ticker) = g.ticker and c.year = date_part('year', fisy.date) -- and c.provided = true 
    left join company."ESGScores" e on e.ticker = g.ticker 
    left join company."Earnings_Annual" ea on ea.ticker = g.ticker and ea.date = cast(fisy.date as text)
    where g.ticker = '{processed_tickers}'

    """

    # print("processing ", processed_tickers )

    # control variable

    result_data_price = await main_db_instance.fetch_rows(query_statement_prices)
    result_data_price = jsonable_encoder(result_data_price)

    # result_data_carbon = await main_db_instance.fetch_rows(query_statement_carbon)
    # result_data_carbon = jsonable_encoder(result_data_carbon)

    result_data_industry = await main_db_instance.fetch_rows(query_statement_industryAVG)
    result_data_industry = jsonable_encoder(result_data_industry)

    result_data_financialStatements = await main_db_instance.fetch_rows(query_statement_financials)
    result_data_financialStatements = jsonable_encoder(result_data_financialStatements)

    industryCarbonDF = pd.DataFrame(result_data_industry)
    # carbonDF = pd.DataFrame(result_data_carbon)
    pricedataDF = pd.DataFrame(result_data_price)
    financialStatements = pd.DataFrame(result_data_financialStatements)
    # print('column values are ', financialStatements.columns.values.tolist())

    industryCarbonDF['ticker'] = 'equity'
    pricedataDF['ticker'] = pricedataDF['ticker'].replace(processed_tickers, 'equity')
    pricedataDF['ticker'] = pricedataDF['ticker'].replace('GSPC.INDX', 'MarketIndex')
    pricedataDF['ticker'] = pricedataDF['ticker'].replace('US10Y.INDX', 'RiskFreeRate')

    # get year and month
    financialStatements['year'] = pd.DatetimeIndex(financialStatements['date']).year
    financialStatements['month'] = pd.DatetimeIndex(financialStatements['date']).month

    # shift the values
    # pricedataDF['date'] = pricedataDF['date'].astype('datetime64[ns]') #set date
    pricedataDFpivot = pricedataDF.pivot(index='date', columns='ticker', values='close')  # pivot table
    pricedataDFpivot['adjustedRF'] = np.log(pricedataDFpivot['RiskFreeRate'] * (1 / 4))
    pricedataDFpivot['index_returns'] = np.log(
        pricedataDFpivot['MarketIndex'] / pricedataDFpivot['MarketIndex'].shift(1))
    pricedataDFpivot['equity_returns'] = np.log(pricedataDFpivot['equity'] / pricedataDFpivot['equity'].shift(1))
    pricedataDFpivot['excess_market_returns'] = pricedataDFpivot['index_returns'] - pricedataDFpivot['adjustedRF']

    # remove missing values
    pricedataDFpivot = pricedataDFpivot.fillna(pricedataDFpivot.rolling(6,
                                                                        min_periods=1).mean())  # replace missing data with 5 day rolling mean. it's 6 because NaN is included
    pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['equity_returns'].notna()]
    pricedataDFpivot = pricedataDFpivot[pricedataDFpivot['excess_market_returns'].notna()]

    # get year of pricing
    pricedataDFpivot['date'] = pricedataDFpivot.index
    pricedataDFpivot['year'] = pd.DatetimeIndex(pricedataDFpivot['date']).year
    pricedataDFpivot['month'] = pd.DatetimeIndex(pricedataDFpivot['date']).month
    pricedataDFpivot = pricedataDFpivot.drop_duplicates(subset=['year', 'month'], keep='first')  # keep only one year

    # print('shape of pricedataframePivot is ',pricedataDFpivot.shape )

    financialStatements = pd.merge(financialStatements, pricedataDFpivot, on=['year', 'month'],
                                   how='left')  # merge industry and company carbon data
    financialStatements = financialStatements.sort_values(['year', 'month'],
                                                          ascending=[True, True])  # sort by year and date
    # financialStatements = financialStatements.drop_duplicates(subset=['year', 'month'], keep='first') #keep only one year
    # print('shape of financialStatements is ',financialStatements.shape )
    # pricedataDFpivot['date'] = pricedataDFpivot.index #save date for pricing
    financialStatements = pd.merge(financialStatements, industryCarbonDF, on='year',
                                   how='outer')  # merge industry and company carbon data

    financialStatements = financialStatements[financialStatements['totalstockholderequity'].notna()]
    # financialStatements = financialStatements[financialStatements['equity'].notna()]
    financialStatements = financialStatements[financialStatements['commonstocksharesoutstanding'].notna()]
    # convert columns to numeric
    # financialStatements = financialStatements[financialStatements[['totalstockholderequity', 'commonstocksharesoutstanding','netincome', 'investments', 'deferredlongtermliab', 'totalassets', 'propertyplantequipment', 'company_carbon' ]].notna()]
    financialStatements[
        ['totalstockholderequity', 'commonstocksharesoutstanding', 'netincome', 'investments', 'deferredlongtermliab',
         'totalassets', 'propertyplantequipment', 'company_carbon']] = financialStatements[
        ['totalstockholderequity', 'commonstocksharesoutstanding', 'netincome', 'investments', 'deferredlongtermliab',
         'totalassets', 'propertyplantequipment', 'company_carbon']].replace(to_replace='None', value=np.nan).dropna()
    financialStatements = financialStatements.dropna(
        subset=['totalstockholderequity', 'commonstocksharesoutstanding', 'netincome', 'investments',
                'deferredlongtermliab', 'totalassets', 'propertyplantequipment', 'company_carbon'], how='all')
    financialStatements[
        ['totalstockholderequity', 'commonstocksharesoutstanding', 'netincome', 'investments', 'deferredlongtermliab',
         'totalassets', 'propertyplantequipment', 'company_carbon']] = financialStatements[
        ['totalstockholderequity', 'commonstocksharesoutstanding', 'netincome', 'investments', 'deferredlongtermliab',
         'totalassets', 'propertyplantequipment', 'company_carbon']].astype(str).astype(float)
    # print(financialStatements[['totalstockholderequity', 'equity', 'commonstocksharesoutstanding' ]])
    financialStatements['booktomarketratio'] = financialStatements['totalstockholderequity'] / (
            financialStatements['equity'] * financialStatements['commonstocksharesoutstanding'])
    financialStatements['returnonequity'] = financialStatements['netincome'] / financialStatements[
        'commonstocksharesoutstanding']  # not dividing by 3 yet as we do not have monthly yet it's quarterly
    financialStatements['investmentsIA'] = financialStatements['investments'] / financialStatements[
        'totalassets']  # not dividing 3
    financialStatements['Leverage'] = financialStatements['deferredlongtermliab'] / financialStatements[
        'totalassets']  # not dividing 3
    financialStatements['logsize'] = np.log(
        financialStatements['equity'] * financialStatements['commonstocksharesoutstanding'])
    financialStatements['logppe'] = np.log(financialStatements['propertyplantequipment'])
    financialStatements['company_carbon'] = financialStatements['company_carbon'] / 4
    financialStatements['company_carbon'] = np.log(financialStatements['company_carbon'])
    financialStatements['equity_returns'] = (financialStatements['equity'] / financialStatements['equity'].shift(1)) - 1
    financialStatements = financialStatements[financialStatements['equity_returns'].notna()]
    financialStatements = financialStatements[financialStatements['company_carbon'].notna()]
    financialStatements = financialStatements[financialStatements['logsize'].notna()]
    financialStatements = financialStatements[financialStatements['investmentsIA'].notna()]
    financialStatements = financialStatements[financialStatements['Leverage'].notna()]
    # print(financialStatements)
    # do regression:
    financialStatements['const'] = 1
    model = sm.OLS(endog=financialStatements['company_carbon'], exog=financialStatements[
        ['const', 'booktomarketratio', 'returnonequity', 'investmentsIA', 'logsize', 'logppe', 'Leverage',
         'excess_market_returns', 'equity_returns']])
    rres = model.fit()

    financialStatements['MSE'] = rres.mse_total  # MSE
    # financialStatements['carbon_alpha_beta'] =  rres.params['alpha'] #beta
    financialStatements['r2'] = rres.rsquared  # r2
    financialStatements['alpha'] = rres.params['const']

    # saving params
    financialStatements['pct_impact_marketcap'] = rres.params['logsize'] * pct_carbon_change
    financialStatements['pct_impact_PPE'] = rres.params['logppe'] * pct_carbon_change
    financialStatements['pct_impact_equity_returns'] = rres.params['equity_returns'] * pct_carbon_change
    financialStatements['pct_impact_ROE'] = rres.params['returnonequity'] * pct_carbon_change
    # pricedataDFpivot = pricedataDFpivot.rename(columns={'company_carbon': 'current_carbon'})
    results = \
        financialStatements[
            ['pct_impact_marketcap', 'pct_impact_PPE', 'pct_impact_equity_returns', 'pct_impact_ROE']].iloc[
            -1].to_dict()

    
    return results  # json.dumps(parsed)
