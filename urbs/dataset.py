from loguru import logger
import pathlib

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



class DataSet(dict):
    def read_data(self, input_path):
        if input_path.suffix == ".xlsx":
            self._read_excel(input_path, 0)
        elif input_path.is_dir():
            if list(input_path.glob("*.xlsx")):
                self._read_excel(input_path, 0)
            elif list(input_path.glob("*.parquet")):
                self._read_parquet(input_path)
            else:
                raise FileNotFoundError(f"{input_path} not valid")
        else:
            raise FileNotFoundError(f"{input_path} not valid")
        return self

    def _read_excel(self, input_path, year):
        import urbs.input as uinput

        self.update(uinput.read_input(input_path, year))

    def _read_parquet(self, input_path):
        logger.debug(f"Input path for parquet: {input_path.absolute()}")
        for filename in input_path.glob("*.parquet"):
            self[filename.stem] = pd.read_parquet(filename)

        return self

    def to_excel(data, output_directory):
        years = list(data["global_prop"].index.levels[0])
        years.sort()
        sheet_name_dict = {
            "global_prop": "Global",
            "supim": "SupIm",
            "dsm": "DSM",
            "eff_factor": "TimeVarEff",
        }

        if type(output_directory) == str:
            output_directory = pathlib.Path(output_directory)
        if not output_directory.is_dir():
            output_directory.mkdir()

        for year in years:
            with pd.ExcelWriter(
                output_directory / f"{int(year)}.xlsx", engine="xlsxwriter"
            ) as writer:
                # write constants to spreadsheet
                # costs.to_frame().to_excel(writer, 'Costs')
                for sheet in data.keys():
                    if sheet in sheet_name_dict.keys():
                        s_name = sheet_name_dict[sheet]
                    else:
                        s_name = "-".join(i.capitalize() for i in sheet.split("_"))
                    if not data[sheet].empty:
                        data[sheet].loc[year].to_excel(
                            writer, s_name, merge_cells=False
                        )

    def to_parquet(data, output_directory: pathlib.Path) -> None:
        """Write urbs dataframes to parquet format.

        Args:
            data: dictionary of dataframes
            output_directory: output directory

        Returns:
            None
        """

        if not output_directory.is_dir():
            output_directory.mkdir()

        for key in data.keys():
            pa.parquet.write_table(
                pa.Table.from_pandas(data[key]), output_directory / f"{key}.parquet"
            )
        logger.info(f"Output data to {output_directory}")

    def data_update(data, **kwargs):
        """scripts to change data in the database.

        Args:
            data: dict of dataframe

        Returns:
            (dict, str): updated data and change_log

        """
        return data



# a class to construct scenarios
class Scenario:
    def __init__(
        self, name="", data_updates=(), constraints=(), doc="", *args, **kwargs
    ):
        if name:
            self.__name__ = name
        self.kwargs = kwargs
        self.doc = doc
        self.data_updates = tuple([*data_updates, *args])
        self.constraints = constraints

    def construct(self, data, **kwargs):
        self.kwargs.update(kwargs)

        for f in self.data_updates:
            f(data, **self.kwargs)

    def update_scenario_data(self, *args, **kwargs):
        if args:
            self.data_updates = tuple([*self.data_updates, *args])
        if kwargs:
            self.kwargs.update(kwargs)
        return self

    def update_scenario_rule(self, *args, **kwargs):
        if args:
            self.constraints = tuple([*self.constraints, *args])
        return self

    def rename_scenario(self, name):
        self.__name__ = name
        return self
