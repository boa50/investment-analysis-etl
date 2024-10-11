def load_roe(df_profit, df_equity, verbose=False):
    df_profit = df_profit.drop(["KPI", "VL_CONTA"], axis=1)
    df_net_worth = df_equity.drop(
        ["DT_INI_EXERC", "KPI", "EXERC_YEAR", "VL_CONTA_ROLLING_YEAR"], axis=1
    )

    df_roe = df_profit.merge(df_net_worth, how="left", on=["CD_CVM", "DT_FIM_EXERC"])

    df_roe["ROE"] = df_roe["VL_CONTA_ROLLING_YEAR"] / df_roe["VL_CONTA"]
    df_roe["VL_CONTA_ROLLING_YEAR"] = -1
    df_roe["VL_CONTA"] = df_roe["ROE"]
    df_roe["KPI"] = "ROE"

    df_roe = df_roe.drop("ROE", axis=1)

    if verbose:
        print()
        print("ROE")
        print(df_roe.head(2))
        print()

    return df_roe
