from db_sessions import main_db_instance
from fastapi.encoders import jsonable_encoder
from sklearn.linear_model import LinearRegression
import pandas as pd
import numpy as np
import json
from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm
import requests
from datetime import datetime

async def getCarbonFootprint(tickers):
    processedTickers = convertTickers(tickers)
    # print("{}".format(processedTickers))
    query_statement = f"select * from company.carbon where upper(ticker) in {processedTickers} order by year ASC"
    result_data = await main_db_instance.fetch_rows(query_statement)
    return jsonable_encoder(result_data)

async def getCarbonBudget(tickers):
    #removing the casting to upper for pricing as items are already upper
    processedTickers = convertTickers(tickers)
    r = requests.get('https://api.15rock.com/fund/GSPC.INDX/holdings', headers={'Authorization': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6ImFub255bW91c0AxNXJvY2suY29tIn0.s_oH_tc3v0iAn9-nzS8ILlu8sYYoC6J8VnQutfCrmKY'})
    indexCarbon = pd.read_json(r.content)
    # print(indexCarbon)
    TotalIndexCarbon = indexCarbon['carbon'].sum()
    indexHoldingsTickers = indexCarbon['ticker'].tolist()
    indexHoldingsTickers = convertTickers(indexHoldingsTickers)
    # print(indexHoldingsTickers)

    #get COGS for index
    # query_statement = f"""
    # SELECT sum(a.totalrevenue -  a.grossprofit) as COGS--a.* 
    # FROM company."financials_Income_Statement_yearly" a
    # INNER JOIN
    # (
    # SELECT ticker, MAX(date) maxDate
    # FROM company."financials_Income_Statement_yearly"
    # WHERE ticker in {indexHoldingsTickers}
    # GROUP BY ticker
    # ) b
    # ON a.ticker = b.ticker
    # AND a.date = b.maxDate
    # """
    query_statement = f"""
    select date_part('year', date) as year, sum(totalrevenue -  grossprofit) as indexcogs
    from company."financials_Income_Statement_yearly" fisy 
    where ticker in {indexHoldingsTickers}
    group by date
    order by date desc 
    """
    result_data = await main_db_instance.fetch_rows(query_statement)
    cogsResults = jsonable_encoder(result_data)
    indexCOGS = pd.DataFrame(cogsResults)
    # indexCOGS = cogsResults[0]['cogs']
    # print(cogsResults[0]['cogs'])

    #get COGS for company
    query_statement = f"""
    select date_part('year', date) as year, sum(totalrevenue -  grossprofit) as companycogs
    from company."financials_Income_Statement_yearly" fisy 
    where ticker in {processedTickers}
    group by date
    order by date desc 
    """
    # query_statement = f"""
    # SELECT sum(a.totalrevenue -  a.grossprofit) as COGS--a.* 
    # FROM company."financials_Income_Statement_yearly" a
    # INNER JOIN
    # (
    # SELECT ticker, MAX(date) maxDate
    # FROM company."financials_Income_Statement_yearly"
    # WHERE ticker in {processedTickers}
    # GROUP BY ticker
    # ) b
    # ON a.ticker = b.ticker
    # AND a.date = b.maxDate
    # """
    result_data = await main_db_instance.fetch_rows(query_statement)
    cogsResults = jsonable_encoder(result_data)
    companyCOGS = pd.DataFrame(cogsResults)
    companyCOGS = companyCOGS.merge(indexCOGS[['year', 'indexcogs']], on='year', how='left')
    # print(companyCOGS)
    # companyCOGS = cogsResults[0]['cogs']
    # print(cogsResults[0]['cogs'])

    #get today's year
    currentYear = datetime.now().year
    yearsRemaining = 2116 - currentYear

    #country carbon
    r = requests.get('https://api.15rock.com/country/United States/carbon/', headers={'Authorization': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6ImFub255bW91c0AxNXJvY2suY29tIn0.s_oH_tc3v0iAn9-nzS8ILlu8sYYoC6J8VnQutfCrmKY'})
    countryCarbon = pd.read_json(r.content)
    # print(countryCarbon)
    # TotalCarbon = countryCarbon['co2emissions'].sum()
    # countryMostRecentCarbon = countryCarbon['co2emissions'].iloc[0]
    # print("most recent carbon is ", countryMostRecentCarbon)
    query_statement = f"""
    select year, sum(co2emissions) as WorldCarbon
    from company.country_emissions ce 
    group by year
    order by year desc
    """
    result_data = await main_db_instance.fetch_rows(query_statement)
    worldEmissions = jsonable_encoder(result_data)
    worldEmissions = pd.DataFrame(worldEmissions)
    # print(worldEmissions)
    countryCarbon = countryCarbon.merge(worldEmissions[['year', 'worldcarbon']], on='year', how='left')
    countryCarbon['emissions_pct'] = countryCarbon['co2emissions']/countryCarbon['worldcarbon']
    # print(countryCarbon)
    averageCountryPCTCarbon = countryCarbon['emissions_pct'].mean()
    # print("average of the country is ", averageCountryPCTCarbon )

    #index's total carbon
    query_statement = f"""
    select year, avg(carbon) as indexavg
    from company.carbon c 
    where upper(ticker) in {indexHoldingsTickers}
    group by "year" 
    order by year desc 
    """
    result_data = await main_db_instance.fetch_rows(query_statement)
    indexAverageCarbon = jsonable_encoder(result_data)
    indexAverageCarbon = pd.DataFrame(indexAverageCarbon)
    # print(indexAverageCarbon)
    countryCarbon = countryCarbon.merge(indexAverageCarbon[['year', 'indexavg']], on='year', how='left')
    countryCarbon['index_pct'] = countryCarbon['indexavg'] / countryCarbon['co2emissions']
    # print(countryCarbon)
    companyCOGS = companyCOGS.merge(countryCarbon, on='year', how='right')
    companyCOGS['companyCarbonBudget'] = (companyCOGS['companycogs'] * companyCOGS['indexcogs'] ) * companyCOGS['index_pct']
    # print(companyCOGS[['companyCarbonBudget']].head(5) )

    #company carbon
    companyCarbon = await getCarbonFootprint(tickers)
    companyCarbon = jsonable_encoder(companyCarbon)
    companyCarbon = pd.DataFrame(companyCarbon)
    companyCarbon = companyCarbon.merge(companyCOGS, on='year', how='right')
    print(companyCarbon[['year', 'carbon', 'companyCarbonBudget']].head(5))
    # companyCarbon = companyCarbon.reset_index()
    # print(companyCarbon)
    # result_data = await main_db_instance.fetch_rows(query_statement)
    # # print(result_data)
    companyCarbon = companyCarbon[['year', 'carbon', 'companyCarbonBudget']].dropna()
    companyCarbon =  companyCarbon[['year', 'carbon', 'companyCarbonBudget']].to_dict('records')
    return companyCarbon
    # parsed = json.loads(companyCarbon)
    # return json.dumps(parsed)
    # return jsonable_encoder(companyCarbon[['year', 'carbon', 'companyCarbonBudget']].to_json())

def main():
    getCarbonBudget("ibm.us")

getCarbonBudget("ibm.us")