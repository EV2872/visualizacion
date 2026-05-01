from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from scripts import test_checks

defs = Definitions(
    assets=load_assets_from_modules([test_checks]),
    asset_checks=load_asset_checks_from_modules([test_checks])
)