import logging
from fastapi import APIRouter, Depends
from models_logic import (
    get_country_tax,
    get_world_carbon_history,
    get_country_carbon_history
)
from fastapi.encoders import jsonable_encoder
from cache.apiroute import CachingLayerRoute
from dependencies.validation import validate_token_dependency

logger = logging.getLogger('COUNTRY_ROUTER')

router = APIRouter(
    prefix='/country',
    tags=['country'],
    responses={404: {'description': 'Not found'}},
    dependencies=[Depends(validate_token_dependency)]
)

router.route_class = CachingLayerRoute

# countryCarbon
@router.get("/{country}/carbon")
async def get_country_carbon_controller(country: str):
    logger.info(f'Getting country carbon for {country}')
    country_carbon = await get_country_carbon_history(country)
    return country_carbon


# country tax
@router.get("/{country}/tax")
async def get_country_tax_regime_controller(
        country: str
):
    logger.info(f'Getting country tax regime for {country}')
    country_tax = await get_country_tax(country)
    return country_tax


# get world - getWorldCarbonHistory
@router.get("/world/carbon/sum")
async def get_world_carbon_history_controller():
    logger.info(f'Getting world carbon history')
    wc_history = await get_world_carbon_history()
    return wc_history
