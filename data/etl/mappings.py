kpi_by_group = {
    "value": ["PRICE_PROFIT", "PRICE_EQUITY", "DIVIDEND_YIELD", "DIVIDEND_PAYOUT"],
    "debt": ["NET_DEBT_BY_EBIT", "NET_DEBT_BY_EQUITY"],
    "efficiency": ["NET_MARGIN", "ROE"],
    "growth": ["CAGR_5_YEARS_PROFIT", "CAGR_5_YEARS_REVENUE"],
}

kpi_fundament_value_column = {
    "EQUITY": "VALUE",
    "NET_REVENUE": "VALUE_ROLLING_YEAR",
    "PROFIT": "VALUE_ROLLING_YEAR",
    "EBIT": "VALUE_ROLLING_YEAR",
    "DEBT": "VALUE",
    "DEBT_NET": "VALUE",
    "NET_MARGIN": "VALUE",
    "ROE": "VALUE",
    "NET_DEBT_BY_EBIT": "VALUE",
    "NET_DEBT_BY_EQUITY": "VALUE",
    "CAGR_5_YEARS_PROFIT": "VALUE",
    "CAGR_5_YEARS_REVENUE": "VALUE",
}

kpi_table_origin = {
    "PRICE": "history",
    "PRICE_PROFIT": "history",
    "PRICE_EQUITY": "history",
    "DIVIDEND_YIELD": "history",
    "DIVIDEND_PAYOUT": "history",
    "EQUITY": "fundaments",
    "NET_REVENUE": "fundaments",
    "PROFIT": "fundaments",
    "EBIT": "fundaments",
    "DEBT": "fundaments",
    "DEBT_NET": "fundaments",
    "NET_MARGIN": "fundaments",
    "ROE": "fundaments",
    "NET_DEBT_BY_EBIT": "fundaments",
    "NET_DEBT_BY_EQUITY": "fundaments",
    "CAGR_5_YEARS_PROFIT": "fundaments",
    "CAGR_5_YEARS_REVENUE": "fundaments",
}
