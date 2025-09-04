# NEM Data Profile

This document provides an overview of key data tables available in the Australian National Electricity Market (NEM) and their implementation status in NEMDataTools. These data sets are essential for analyzing electricity prices, demand, generation, and forecasts within the Australian electricity market.

> **Implementation Status**: NEMDataTools now provides production-ready access to core MMSDM data types with comprehensive processing and validation.

## NEM Data Types

Two primary data types are available in the NEM:
1. Data Reports - status can be `CURRENT` or `ARCHIVE`
    - `REPORTS_URL_TEMPLATE = f"{REPORTS_BASE_URL}{{status}}/{{package}}/{{file_name}}.zip"`
2. Archived Data Tables in MMSDM
    - `MMSDM_URL_TEMPLATE = f"{MMSDM_BASE_URL}{{year}}/MMSDM_{{year}}_{{month}}/MMSDM_Historical_Data_SQLLoader/DATA/{{file_name}}.zip"`

NEMDataTools provides comprehensive support for MMSDM archive data with additional support for pre-dispatch and static data sources.

### MMSDM URL Structure

The base URL for the MMSDM archive is:
```python
MMSDM_BASE_URL = "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
```

Then, we need to specify the year and month for the data table, for example:
```python
MMSDM_URL_TEMPLATE = f"{MMSDM_BASE_URL}{year}/MMSDM_{year}_{month}/MMSDM_Historical_Data_SQLLoader/DATA/{file_name}.zip"
```

The `file_name` is the name of the zipped file containing the data table, but it is a little bit complicated to get the file name. Since the file name is not same with the table name, we need to write a function to construct the file name from the table name.

Example:
```python
table_name = "DISPATCHPRICE"
# Before Aug 2024, the file name is in the format of:
# PUBLIC_DVD_DISPATCHPRICE_{year}{month}010000
file_name = f"PUBLIC_DVD_{table_name}_{year}{month}010000"

# Since Aug 2024, the file name is changed to the format of:
# PUBLIC_ARCHIVE#DISPATCHPRICE#FILE01#{year}{month}010000.zip
file_name = f"PUBLIC_ARCHIVE#{table_name}#FILE01#{year}{month}010000"
```

### Special Cases

- PREDISPATCHPRICE not available in 2022-10, folder `PREDISP_ALL_DATA/`
    - https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2022/MMSDM_2022_10/MMSDM_Historical_Data_SQLLoader/PREDISP_ALL_DATA/

## AEMO Data Tables

These tables are publicly available on the [NEMWEB](https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb) portal.

| Table Name | Package | Description | Key Fields |
|------------|---------|-------------|------------|
| DISPATCHPRICE | DISPATCH | 5-minute dispatch prices by region | SETTLEMENTDATE, REGIONID, RRP |


## Regional Reference Nodes

Prices in the NEM are calculated at the following Regional Reference Nodes (RRNs):

| Region ID | Description | Regional Reference Node |
|-----------|-------------|-------------------------|
| NSW1 | New South Wales | Sydney West 330kV |
| QLD1 | Queensland | South Pine 275kV |
| SA1 | South Australia | Torrens Island 66kV |
| TAS1 | Tasmania | Georgetown 220kV |
| VIC1 | Victoria | Thomastown 66kV |

## Data Access Frequency

| Data Type | Update Frequency | Historical Availability |
|-----------|------------------|-------------------------|
| DISPATCH | Every 5 minutes | 2009-present |
| PREDISPATCH | Every 30 minutes | 2009-present |
| P5MIN | Every hour | 2016-present |
| TRADING | Every 30 minutes (discontinued Oct 2021) | 2009-2021 |
| BIDDING | Daily | 2009-present |
| SCADA | Every 5 minutes | Limited public access |

## Key Field Descriptions

### Common Fields
- **SETTLEMENTDATE**: Timestamp for the dispatch interval (5-minute resolution)
- **DATETIME** or **INTERVAL_DATETIME**: Timestamp for forecasted intervals
- **REGIONID**: NEM region code (NSW1, QLD1, SA1, TAS1, VIC1)
- **DUID**: Dispatchable Unit Identifier for generators or loads
- **RRP**: Regional Reference Price in $/MWh
- **TOTALDEMAND**: Regional total demand in MW

### Generation and Dispatch Fields
- **INITIALMW**: Initial MW output at the start of a dispatch interval
- **TOTALCLEARED**: Final MW target at the end of a dispatch interval
- **AVAILABILITY**: Maximum available capacity for dispatch
- **AGCSTATUS**: Automatic Generation Control status (on/off)

### Bidding Fields
- **BANDAVAIL1-10**: Available capacity in each of 10 price bands (MW)
- **PRICEBAND1-10**: Price offered for each price band ($/MWh)
- **MAXAVAIL**: Maximum availability for the dispatch interval
- **ROCUP/ROCDOWN**: Rate of change limits (MW/minute)

## Data Model Relationships

The key relationships between tables in the NEM data model are:

1. **Regional Data**: DISPATCHPRICE, DISPATCHREGIONSUM, PREDISPATCHPRICE, and PREDISPATCHREGIONSUM all share the region identifier (REGIONID)

2. **Unit Data**: DISPATCHUNIT, DISPATCHLOAD, and BIDDAYOFFER share the dispatchable unit identifier (DUID)

3. **Time Relationships**:
   - DISPATCH data is at 5-minute resolution
   - PREDISPATCH data is at 30-minute resolution
   - Trading data (pre-Oct 2021) was at 30-minute resolution

Please refer to the [MMS Data Model](https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/Electricity%20Data%20Model%20Report.htm) for detailed information on the data structure and definitions.

## Recent Changes

- **Five Minute Settlement**: On October 1, 2021, the NEM transitioned from 30-minute to 5-minute settlement, eliminating the need for separate TRADING tables
- **Wholesale Demand Response**: Introduced in October 2021 to allow demand-side participation
- **Global Settlement**: Replaced "settlement by difference" for retailers as of May 2022

## NEMDataTools Implementation Status

### ✅ Fully Supported Data Types

| Data Type | Description | Processing Features |
|-----------|-------------|-------------------|
| `DISPATCHPRICE` | 5-minute dispatch prices by region | Price validation, intervention handling, regional indexing |
| `DISPATCHREGIONSUM` | 5-minute regional dispatch summary | Demand aggregation, multi-index support |
| `DISPATCH_UNIT_SCADA` | Generator SCADA readings | Unit-level output processing, time series indexing |
| `PREDISPATCHPRICE` | Pre-dispatch price forecasts | Forecast horizon calculations, run-time indexing |
| `PRICE_AND_DEMAND` | Direct CSV price/demand data | Regional filtering, time series processing |

### ⚠️ Framework Ready (Implementation Complete, Testing Pending)

| Data Type | Description | Status |
|-----------|-------------|--------|
| `P5MIN_REGIONSOLUTION` | 5-minute pre-dispatch region solution | Parser implemented, comprehensive testing needed |
| Static Data Types | Registration lists, boundaries | Framework established, validation pending |

### 📋 Configured But Not Yet Tested

The following data types have URL mappings and parser configurations but require validation:
- `DISPATCHLOAD`, `DISPATCHINTERCONNECTORRES`
- `BIDDAYOFFER_D`, `BIDPEROFFER_D`
- `DUDETAILSUMMARY`, `GENCONDATA`
- `MARKETNOTICEDATA`, `NETWORK_OUTAGEDETAIL`
- `ROOFTOPPV_ACTUAL`, `TRADINGREGIONSUM`
- `PREDISPATCHREGIONSUM`, `PREDISPATCHLOAD`
- `P5MIN_INTERCONNECTORSOLN`

## Data Quality Considerations

NEMDataTools handles many common data quality issues automatically:

1. **Intervention Periods**: ✅ Automatically flagged and handled in processing
2. **Data Standardization**: ✅ Consistent column naming and data type conversion
3. **Missing Data**: ✅ Proper NaN handling and validation
4. **Time Zone Handling**: ✅ Correct AEST/AEDT conversion and indexing
5. **Regional Consistency**: ✅ Standardized region codes and filtering
