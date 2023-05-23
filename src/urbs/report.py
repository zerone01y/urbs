import pathlib
import sqlite3 as sql
import pandas as pd
import pyomo.core
from loguru import logger

from .output import (
    get_constants,
    get_all_timeseries,
    get_timeseries,
    entity_to_sql,
    get_input,
)
from .util import is_string


def input_data_report(prob, conn, sce):
    """Write input data summary to a sqlite database

    Args:
        - prob: a urbs model instance;
        - filename: Excel spreadsheet filename, will be overwritten if exists;
    """

    # matrix to list
    prob._data["demand"] = prob._data["demand"].stack(0)
    # set weight in global_prop
    prob._data["global_prop"].loc[
        (prob._data["global_prop"].index.levels[0][0], "DTWeight"), "value"
    ] = prob.weight()

    for df in prob._data:
        # drop duplicated columns
        if df not in ["supim"]:
            for c in prob._data[df].columns.intersection(prob._data[df].index.names):
                prob._data[df].drop([c], axis=1, inplace=True)
        if not prob._data[df].empty:
            prob._data[df]["Scenario"] = sce
        prob._data[df].to_sql("Input_" + df, conn, if_exists="append")

    return


def report(instance, filename, report_tuples=None, report_sites_name={}):
    """Write result summary to a spreadsheet file

    Args:
        - instance: a urbs model instance;
        - filename: Excel spreadsheet filename, will be overwritten if exists;
        - report_tuples: (optional) list of (sit, com) tuples for which to
          create detailed timeseries sheets;
        - report_sites_name: (optional) dict of names for created timeseries
          sheets
    """

    # default to all demand (sit, com) tuples if none are specified
    if report_tuples is None:
        report_tuples = get_input(instance, "demand").columns

    costs, cpro, ctra, csto = get_constants(instance)

    # create spreadsheet writer object
    with pd.ExcelWriter(filename) as writer:
        # write constants to spreadsheet
        costs.to_frame().to_excel(writer, "Costs")
        cpro.to_excel(writer, "Process caps")
        ctra.to_excel(writer, "Transmission caps")
        csto.to_excel(writer, "Storage caps")

        # initialize timeseries tableaus
        energies = []
        timeseries = {}
        help_ts = {}

        # collect timeseries data
        for stf, sit, com in report_tuples:
            # wrap single site name in 1-element list for consistent behavior
            if is_string(sit):
                help_sit = [sit]
            else:
                help_sit = sit
                sit = tuple(sit)

            # check existence of predefined names, else define them
            try:
                report_sites_name[sit]
            except BaseException:
                report_sites_name[sit] = str(sit)

            for lv in help_sit:
                (
                    created,
                    consumed,
                    stored,
                    imported,
                    exported,
                    dsm,
                    voltage_angle,
                ) = get_timeseries(instance, stf, com, lv)

                overprod = pd.DataFrame(
                    columns=["Overproduction"],
                    data=created.sum(axis=1)
                    - consumed.sum(axis=1)
                    + imported.sum(axis=1)
                    - exported.sum(axis=1)
                    + stored["Retrieved"]
                    - stored["Stored"],
                )

                tableau = pd.concat(
                    [
                        created,
                        consumed,
                        stored,
                        imported,
                        exported,
                        overprod,
                        dsm,
                        voltage_angle,
                    ],
                    axis=1,
                    keys=[
                        "Created",
                        "Consumed",
                        "Storage",
                        "Import from",
                        "Export to",
                        "Balance",
                        "DSM",
                        "Voltage Angle",
                    ],
                )
                help_ts[(stf, lv, com)] = tableau.copy()

                # timeseries sums
                help_sums = pd.concat(
                    [
                        created.sum(),
                        consumed.sum(),
                        stored.sum().drop("Level"),
                        imported.sum(),
                        exported.sum(),
                        overprod.sum(),
                        dsm.sum(),
                    ],
                    axis=0,
                    keys=[
                        "Created",
                        "Consumed",
                        "Storage",
                        "Import",
                        "Export",
                        "Balance",
                        "DSM",
                    ],
                )
                try:
                    timeseries[(stf, report_sites_name[sit], com)] = timeseries[
                        (stf, report_sites_name[sit], com)
                    ].add(help_ts[(stf, lv, com)], axis=1, fill_value=0)
                    sums = sums.add(help_sums, fill_value=0)
                except BaseException:
                    timeseries[(stf, report_sites_name[sit], com)] = help_ts[
                        (stf, lv, com)
                    ]
                    sums = help_sums

            # timeseries sums
            sums = pd.concat(
                [
                    created.sum(),
                    consumed.sum(),
                    stored.sum().drop("Level"),
                    imported.sum(),
                    exported.sum(),
                    overprod.sum(),
                    dsm.sum(),
                ],
                axis=0,
                keys=[
                    "Created",
                    "Consumed",
                    "Storage",
                    "Import",
                    "Export",
                    "Balance",
                    "DSM",
                ],
            )
            energies.append(sums.to_frame("{}.{}.{}".format(stf, sit, com)))

        # write timeseries data (if any)
        if timeseries:
            # concatenate Commodity sums
            energy = pd.concat(energies, axis=1).fillna(0)
            energy.to_excel(writer, "Commodity sums")

            # write timeseries to individual sheets
            for stf, sit, com in report_tuples:
                if isinstance(sit, list):
                    sit = tuple(sit)
                # sheet names cannot be longer than 31 characters...
                sheet_name = "{}.{}.{} timeseries".format(
                    stf, report_sites_name[sit], com
                )[:31]
                timeseries[(stf, report_sites_name[sit], com)].to_excel(
                    writer, sheet_name
                )


def report_all(
    instance: pyomo.core.ConcreteModel,
    resultdir: pathlib.Path,
    sce: str,
    input_report: bool = True,
    *args,
    **kwargs,
):
    """Write result summary to spreadsheet files

    Args:
        - instance: a urbs model instance;
        - resultdir: output dir
        - filename: Excel spreadsheet filename, will be overwritten if exists;
        - sce: scenario name;
        - input_report: export input parameter as database
    Output:
        A spreadsheet named by scenario, including annual level data for Trans, Proc and Cost
        Other variables and dual info is stored in sqlite database.
    """

    merge_cells = False

    if len(instance.timesteps) > 1:
        """
        created:
            Columns: [<Technologies>]
            Index: ['t', 'stf', 'sit', 'com']
        transmitted:
            Columns: ['exported', 'imported', 'residue', 'utilization']
            Index: ['t', 'stf', 'sit', 'sit_', 'tra', 'com']
        balance:
            matrix of created by tech, exported, imported

            Columns: [<Technologies>, imports, exports, Storage(charging), Storage(discharging)]
            Index: ['t', 'Stf', 'Site', 'Process']
        """

        created, transmitted, stored, balance = get_all_timeseries(instance)

        filename = resultdir / "Scenarios"
        if not filename.is_dir():
            filename.mkdir()
        balance.assign(Scenario=sce).to_csv(
            filename / (f"timeseries_{sce}_balance_.csv")
        )
        if not transmitted.empty:
            transmitted.assign(Scenario=sce).to_csv(
                filename / (f"timeseries_{sce}_transmitted_timeseries.csv")
            )
        if not stored.empty:
            stored.assign(Scenario=sce).to_csv(
                filename / f"timeseries_{sce}_storage_timeseries.csv"
            )

    # ==============================================================================
    # ===============Annual data====================================================
    # Annual level (constants for each year)
    costs, cpro, ctra, csto = get_constants(
        instance
    )  # overall costs by type, capacity (total and new) for pro, tra, sto

    logger.info(f"{sce}: {costs}")

    """cpro: annual level data for each process or trade (import, export)
    index = ['Year', 'Site', 'Process']
    column: capacity (total, new); Generation (CO2, Elec); Cost(Inv, Fix, Var, Fuel)
    """
    balance = (
        balance.unstack(0)
        .sum(level=0, axis=1)
        .rename_axis(index=["Stf", "Site", "Process"])
    ) * instance.weight()  # sum timeseries over t to obtain annual results

    cpro = pd.concat([cpro, balance], axis=1, keys=("Capacity", "Generation")).fillna(0)
    cpro = cpro.loc[(cpro != 0).any(axis=1)]
    cpro["Scenario"] = sce
    cpro.columns = cpro.columns.map(".".join).str.strip(".")

    if instance.mode["tra"]:
        """ctra
        index = ['Year', 'From', 'To', 'Transmission', 'Direction']
        columns = capacity (total and new),
                transmission of two directions (exported, imported(after loss),
                scenario
        """

        # sum transmission data over t
        transmitted = (
            transmitted[["exported", "imported"]].unstack(0).sum(axis=1, level=0)
        ) * instance.weight()
        assert transmitted.shape[0] == ctra.shape[0]
        ctra = (
            pd.concat([ctra, transmitted], axis=1, keys=["Capacity", "Transmitted"])
            .fillna(0)
            .reset_index()
        )  # concat transmission throughput info to ctra

        # One row for Each physical line, show the throughput of both directions of the transmission lines.

        ctra = (
            (
                ctra.loc[ctra["Site In"] < ctra["Site Out"]]
                .rename(columns={"Transmitted": "Forward"})
                .merge(
                    ctra.loc[ctra["Site In"] > ctra["Site Out"]].rename(
                        columns={
                            "Site In": "Site Out",
                            "Site Out": "Site In",
                            "Transmitted": "Backward",
                        }
                    ),
                    on=[
                        ("Stf", ""),
                        ("Site In", ""),
                        ("Site Out", ""),
                        ("Transmission", ""),
                        ("Commodity", ""),
                    ],
                    sort=True,
                    suffixes=("", "_"),
                )
                .set_index(["Stf", "Site In", "Site Out", "Transmission", "Commodity"])
                .rename_axis(index={"Site In": "From", "Site Out": "To"})
            )
            .sort_index(axis=1)
            .drop(columns="Capacity_")
        )

        ctra["Scenario"] = sce
        # combine two levels of column index
        ctra.columns = ctra.columns.map(".".join).str.strip(".")
    else:
        ctra = pd.DataFrame(
            columns=[
                "Year",
                "From",
                "To",
                "Transmission",
                "Commodity",
                "Capacity.Total",
                "Capacity.New",
                "Forward.exported",
                "Forward.imported",
                "Backward.exported",
                "Backward.imported",
                "Scenario",
            ]
        )
    if instance.mode["sto"]:
        stored = (stored.unstack(0).sum(axis=1, level=0)) * instance.weight()
        csto = pd.concat([csto, stored], axis=1, keys=["Capacity", "Stored"]).fillna(0)
        csto["Scenario"] = sce
        csto.columns = csto.columns.map(".".join).str.strip(".")
    else:
        csto = pd.DataFrame(
            columns=[
                "Year",
                "Site",
                "Storage",
                "Commodity",
                "Capacity.C Total",
                "Capacity.C New",
                "Capacity.P Total",
                "Capacity.P New",
                "Stored.Level",
                "Stored.Storage(charging)",
                "Stored.Storage(discharging)",
                "Scenario",
            ]
        )
    # Output to excel
    filename = resultdir / "Scenarios"
    if not filename.is_dir():
        filename.mkdir()
    filename = filename / f"{sce}_summary.xlsx"
    with pd.ExcelWriter(filename) as writer:
        # write constants to spreadsheet
        # costs.to_frame().to_excel(writer, 'Costs')
        costs.to_excel(writer, "Costs", merge_cells=merge_cells)
        cpro.to_excel(writer, "Proc", merge_cells=merge_cells)
        # Capacity by technology by country by year, two structures:
        if instance.mode["tra"]:
            ctra.to_excel(writer, "Trans", merge_cells=merge_cells)
        if instance.mode["sto"]:
            csto.to_excel(writer, "Storage", merge_cells=merge_cells)

    """write to sqlite database
    
    data.db
    - Proc: annual level data for each process (and export, import), 
            including capacity, annual generation, and costs
    - Cost: annual level total costs by year and type
    - Trans: annual level transmission line data
            including capacity, annual throughput, and costs 
            costs are divided by 2 to ensure that it sums up to the correct value
    - Other variables: e_co_stock(commodity), e_pro_in (input), e_pro_out (output)
    - Dual variables for constraints:
        'res_vertex': balance for each node (time step, node) - marginal generation cost
        'res_env_total': dual var for emission constraint (per year, node) - marginal abatement cost
        'res_process_throughput_by_capacity': shadow price to add extra capacity for each tech
        'res_transmission_input_by_capacity': shadow price to add extra capacity for each line
    """

    with sql.connect(str(resultdir / "data.db")) as conn:
        # Annual summary
        cpro.to_sql("Result_Proc", conn, if_exists="append")
        ctra.to_sql("Result_Trans", conn, if_exists="append")
        csto.to_sql("Result_Storage", conn, if_exists="append")
        # Vars
        for var in [
            "e_co_stock",
            "e_pro_in",
            "e_pro_out",
            "e_tra_in",
            "e_tra_out",
            "e_sto_in",
            "e_sto_out",
            "e_sto_con",
            "e_co_buy",
            "e_co_sell",
            "tau_pro",
            "cap_pro_new",
            "cap_tra_new",
        ]:
            # commodity consumption, process in, process out, transmission from, transmission to,
            if hasattr(instance, var):
                entity_to_sql(instance, var, conn, sce, "Var")
        if hasattr(instance, "dual"):
            for constraint in [
                "res_vertex",
                "res_env_total",
                "res_process_throughput_by_capacity",
                "res_transmission_input_by_capacity",
                "res_process_capacity",
                "def_intermittent_supply",
            ]:
                entity_to_sql(instance, constraint, conn, sce, "Dual")

        if input_report:
            input_data_report(instance, conn, sce)
