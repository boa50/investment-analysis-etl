import pandas as pd

year_initial = 2011
year_final = 2024
years_load = list(range(year_initial, year_final + 1))

file_categories_loaded = []

for year in years_load:
    file_categories_loaded.append("itr_" + str(year))
    file_categories_loaded.append("dfp_" + str(year))

df_reference_table = pd.read_csv("data/processed/reference-table.csv")

### get_kpi_fields
# print(file_categories_loaded)

df_reference_table_tmp = df_reference_table[df_reference_table["KPI"] == "PROFIT"]
df_reference_table_tmp.loc[:, "CD_CONTA"] = df_reference_table_tmp["CD_CONTA"].astype(
    str
)

# df_kpi = pd.DataFrame()

df_reference_table = df_reference_table_tmp

for cd_cvm in df_reference_table_tmp["CD_CVM"].unique():
    # df_kpi = pd.concat(
    #     [
    #         df_kpi,
    #         get_kpi_by_cvm_code(
    #             df, cd_cvm, kpi_name, file_categories_loaded, df_reference_table_tmp
    #         ),
    #     ]
    # )

    ### get_kpi_by_cvm_code
    df_reference_table_tmp = df_reference_table[df_reference_table["CD_CVM"] == cd_cvm]

    cds_conta_reference = df_reference_table_tmp[
        df_reference_table_tmp["CD_CONTA"] != "-1"
    ][["CD_CVM", "FILE_PERIOD", "YEAR_START", "YEAR_END", "CD_CONTA"]]

    cds_conta = pd.DataFrame()

    for _, row in cds_conta_reference.iterrows():
        year_start = row["YEAR_START"]
        year_end = row["YEAR_END"]
        file_period = row["FILE_PERIOD"]

        for fcategory in file_categories_loaded:
            fcategory_year = int(fcategory[-4:])
            fcategory_period = fcategory[:3]

            is_same_period = (file_period == "all") or (file_period == fcategory_period)
            is_in_year = (fcategory_year >= year_start) and (
                (fcategory_year <= year_end) or (year_end == -1)
            )

            if is_same_period and is_in_year:
                cds_conta = pd.concat(
                    [
                        cds_conta,
                        pd.DataFrame(
                            {
                                "CD_CVM": row["CD_CVM"],
                                "FILE_CATEGORY_SHORT": fcategory,
                                "CD_CONTA": str(row["CD_CONTA"]),
                                "MATCHED_CD": True,
                            },
                            index=[cds_conta.shape[0]],
                        ),
                    ]
                )

    # print(cds_conta)

    dss_conta = df_reference_table_tmp[df_reference_table_tmp["DS_CONTA"] != "-1"][
        ["CD_CVM", "FILE_PERIOD", "YEAR_START", "DS_CONTA"]
    ]
    dss_conta["MATCHED_DS"] = True

    dss_conta["YEAR_START"] = dss_conta["YEAR_START"].astype(int).astype(str)

    dss_conta["FILE_CATEGORY_SHORT"] = (
        dss_conta["FILE_PERIOD"] + "_" + dss_conta["YEAR_START"]
    )

    print(dss_conta)

    # general_cd_conta_value = df_reference_table_tmp[
    #     df_reference_table_tmp["FILE_CATEGORY_SHORT"] == "-1"
    # ][["CD_CVM", "CD_CONTA", "YEAR_START", "YEAR_END"]]

    # distinct_files = df_reference_table_tmp[
    #     df_reference_table_tmp["FILE_CATEGORY_SHORT"] != "-1"
    # ]
    # distinct_files_categories = distinct_files["FILE_CATEGORY_SHORT"].values

    # distinct_files_cd_conta = distinct_files[distinct_files["CD_CONTA"] != "-1.0"][
    #     ["CD_CVM", "FILE_CATEGORY_SHORT", "CD_CONTA"]
    # ]
    # distinct_files_cd_conta["MATCHED_2"] = True

    # distinct_files_ds_conta = distinct_files[distinct_files["DS_CONTA"] != "-1"][
    #     ["CD_CVM", "FILE_CATEGORY_SHORT", "DS_CONTA"]
    # ]
    # distinct_files_ds_conta["MATCHED_3"] = True

    # general_files = list(
    #     set(file_categories_loaded).difference(distinct_files_categories)
    # )
    # general_files.sort()

    # general_cd_conta = pd.DataFrame()

    # for _, row in general_cd_conta_value.iterrows():
    #     year_start = row["YEAR_START"]
    #     year_end = row["YEAR_END"]

    #     for fcategory in general_files:
    #         fcategory_year = int(fcategory[-4:])

    #         if (fcategory_year >= year_start) and (
    #             (fcategory_year <= year_end) or (year_end == -1)
    #         ):
    #             general_cd_conta = pd.concat(
    #                 [
    #                     general_cd_conta,
    #                     pd.DataFrame(
    #                         {
    #                             "CD_CVM": row["CD_CVM"],
    #                             "FILE_CATEGORY_SHORT": fcategory,
    #                             "CD_CONTA": str(row["CD_CONTA"]),
    #                             "MATCHED_1": True,
    #                         },
    #                         index=[general_cd_conta.shape[0]],
    #                     ),
    #                 ]
    #             )

    # print(general_cd_conta)

    # df_kpi = df.merge(
    #     general_cd_conta, how="left", on=["CD_CVM", "FILE_CATEGORY_SHORT", "CD_CONTA"]
    # )
    # df_kpi = df_kpi.merge(
    #     distinct_files_cd_conta,
    #     how="left",
    #     on=["CD_CVM", "FILE_CATEGORY_SHORT", "CD_CONTA"],
    # )
    # df_kpi = df_kpi.merge(
    #     distinct_files_ds_conta,
    #     how="left",
    #     on=["CD_CVM", "FILE_CATEGORY_SHORT", "DS_CONTA"],
    # )

    # df_kpi = df_kpi[df_kpi[["MATCHED_1", "MATCHED_2", "MATCHED_3"]].any(axis=1)]
    # df_kpi["KPI"] = kpi_name

    # return df_kpi[
    #     [
    #         "CD_CVM",
    #         "DT_INI_EXERC",
    #         "DT_FIM_EXERC",
    #         "KPI",
    #         "VL_CONTA",
    #         "EXERC_YEAR",
    #     ]
    # ]

    break
