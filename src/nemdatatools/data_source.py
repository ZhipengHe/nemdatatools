"""NEMDataTools - Constants and configuration for AEMO data sources.

This module defines the data sources, URL patterns, and supported tables
for the Australian Energy Market Operator (AEMO) data.
"""

import enum
from typing import Final, Literal, TypedDict


class DataSource(enum.Enum):
    """Enumeration of available AEMO data sources."""

    PRICE_AND_DEMAND = "PRICE_AND_DEMAND"  # Direct CSV download service
    REPORTS_CURRENT = "REPORTS_CURRENT"  # Current reports
    REPORTS_ARCHIVE = "REPORTS_ARCHIVE"  # Archive reports
    MMSDM = "MMSDM"  # MMS Data Model historical database (DATA folder)
    MMSDM_PREDISP = (
        "MMSDM_PREDISP"  # MMS Data Model pre-dispatch data (PREDISP_ALL_DATA folder)
    )
    MMSDM_P5MIN = (
        "MMSDM_P5MIN"  # MMS Data Model 5-min pre-dispatch data (P5MIN_ALL_DATA folder)
    )
    STATIC = "STATIC"  # Static reference data


# Base URLs for different data sources
BASE_URLS: Final = {
    "NEMWEB": "http://nemweb.com.au/",
    "REPORTS": "http://nemweb.com.au/Reports/",
    "MMSDM": "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/",
    "AEMO": "https://aemo.com.au/",
}


# URL templates for different data sources
URL_TEMPLATES: Final = {
    DataSource.REPORTS_CURRENT: (
        f"{BASE_URLS['REPORTS']}CURRENT/{{package}}/{{file_name}}"
    ),
    DataSource.REPORTS_ARCHIVE: (
        f"{BASE_URLS['REPORTS']}ARCHIVE/{{package}}/{{file_name}}"
    ),
    DataSource.MMSDM: (
        f"{BASE_URLS['MMSDM']}{{year}}/MMSDM_{{year}}_{{month}}/"
        f"MMSDM_Historical_Data_SQLLoader/DATA/{{filename}}.zip"
    ),
    DataSource.MMSDM_PREDISP: (
        f"{BASE_URLS['MMSDM']}{{year}}/MMSDM_{{year}}_{{month}}/"
        f"MMSDM_Historical_Data_SQLLoader/PREDISP_ALL_DATA/{{filename}}.zip"
    ),
    DataSource.MMSDM_P5MIN: (
        f"{BASE_URLS['MMSDM']}{{year}}/MMSDM_{{year}}_{{month}}/"
        f"MMSDM_Historical_Data_SQLLoader/P5MIN_ALL_DATA/{{filename}}.zip"
    ),
    DataSource.PRICE_AND_DEMAND: (
        f"{BASE_URLS['AEMO']}aemo/data/nem/priceanddemand/"
        f"PRICE_AND_DEMAND_{{yearmonth}}_{{region}}.csv"
    ),
}


# NEM regions
NEM_REGIONS: Final = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]


class DataConfigEntryRequired(TypedDict):
    """Required fields for data configuration entries.

    Contains the mandatory fields that every data source configuration
    must include.
    """

    source: DataSource
    format: Literal["csv", "xlsx", "xls", "zip"]
    parser_function: str
    description: str


class DataConfigEntry(DataConfigEntryRequired, total=False):
    """Configuration entry for a data source.

    Extends the required fields with optional fields like URL.
    """

    url: str  # Required for static data sources


# Configuration for data types
DATA_CONFIG: Final[dict[str, DataConfigEntry]] = {
    # Price and Demand API
    "PRICE_AND_DEMAND": {
        "source": DataSource.PRICE_AND_DEMAND,
        "format": "csv",
        "parser_function": "_parse_price_and_demand",
        "description": "Direct price and demand data by region and month",
    },
    # MMS Data Model tables - Standard DATA directory
    "DISPATCHPRICE": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_dispatch_price",
        "description": "5-minute dispatch prices by region",
    },
    "DISPATCHREGIONSUM": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_dispatch_region_sum",
        "description": "5-minute regional dispatch summary including demand",
    },
    "DISPATCH_UNIT_SCADA": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_dispatch_unit_scada",
        "description": "5-minute actual generator output from SCADA readings",
    },
    "DISPATCHLOAD": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_dispatch_load",
        "description": "5-minute dispatch targets for each generator",
    },
    "DISPATCHINTERCONNECTORRES": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_dispatch_interconnector_res",
        "description": "5-minute interconnector flows between regions",
    },
    "BIDDAYOFFER_D": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_bid_day_offer",
        "description": "Daily energy bid offers",
    },
    "DUDETAILSUMMARY": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_du_detail_summary",
        "description": "Dispatch unit details including unit capabilities",
    },
    "GENCONDATA": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_gencon_data",
        "description": "Generator constraint data",
    },
    "MARKETNOTICEDATA": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_market_notice_data",
        "description": "Market notices published by AEMO",
    },
    "BIDPEROFFER_D": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_bid_per_offer",
        "description": "Detailed price band offers",
    },
    "DISPATCHCONSTRAINT": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_dispatch_constraint",
        "description": "5-minute constraint solution data",
    },
    "NETWORK_OUTAGEDETAIL": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_network_outage_detail",
        "description": "Network outage details",
    },
    "ROOFTOPPV_ACTUAL": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_rooftop_pv_actual",
        "description": "Estimated rooftop PV generation",
    },
    "TRADINGREGIONSUM": {
        "source": DataSource.MMSDM,
        "format": "zip",
        "parser_function": "_parse_trading_region_sum",
        "description": "Trading interval regional summary",
    },
    # MMS Data Model tables - PREDISP_ALL_DATA directory
    "PREDISPATCHPRICE": {
        "source": DataSource.MMSDM_PREDISP,
        "format": "zip",
        "parser_function": "_parse_predispatch_price",
        "description": "Pre-dispatch prices by region",
    },
    "PREDISPATCHREGIONSUM": {
        "source": DataSource.MMSDM_PREDISP,
        "format": "zip",
        "parser_function": "_parse_predispatch_region_sum",
        "description": "Pre-dispatch regional summary",
    },
    "PREDISPATCHLOAD": {
        "source": DataSource.MMSDM_PREDISP,
        "format": "zip",
        "parser_function": "_parse_predispatch_load",
        "description": "Pre-dispatch load targets",
    },
    # MMS Data Model tables - P5MIN_ALL_DATA directory
    "P5MIN_REGIONSOLUTION": {
        "source": DataSource.MMSDM_P5MIN,
        "format": "zip",
        "parser_function": "_parse_p5min_region_solution",
        "description": "5-minute pre-dispatch region solution",
    },
    "P5MIN_INTERCONNECTORSOLN": {
        "source": DataSource.MMSDM_P5MIN,
        "format": "zip",
        "parser_function": "_parse_p5min_interconnector_soln",
        "description": "5-minute pre-dispatch interconnector solution",
    },
    # Static reference data
    "NEM_REG_AND_EXEMPTION": {
        "source": DataSource.STATIC,
        "url": "https://www.aemo.com.au/-/media/files/electricity/nem/participant_information/nem-registration-and-exemption-list.xlsx",
        "format": "xlsx",
        "parser_function": "_parse_registration_list",
        "description": "NEM Registration and Exemption List with generator details",
    },
    "REGION_BOUNDARIES": {
        "source": DataSource.STATIC,
        "url": "https://www.aemo.com.au/-/media/files/electricity/nem/data/nem-region-boundaries.xlsx",
        "format": "xlsx",
        "parser_function": "_parse_region_boundaries",
        "description": "NEM region boundaries information",
    },
}
