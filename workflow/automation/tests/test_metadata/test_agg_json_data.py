"""
Unit tests for aggregation of metadata from simulation json files.
"""
import sys
import os
import json
import argparse

import pytest
import numpy as np
import pandas as pd

from workflow.automation.metadata.agg_json_data import (
    main,
    create_dataframe,
    load_metadata_df,
    DATE_COLUMNS,
)
import qcore.constants as const


class TestAggJsonData:
    test_dir = "./metadata_test_dir"
    test_output_file = "metadata.csv"
    test_input_file_template = "log_{}.json"

    sim_name_1 = "simulation_1"
    sim_name_2 = "simulation_2"
    sim_name_3 = "simulation_3"
    json_files_content = {
        1: {
            "sim_name": sim_name_1,
            "HF": {
                "submit_time": "2019-02-05_01:30:59",
                "cores": 80,
                "fd_count": 586,
                "start_time": "2019-02-05_01:31:03",
                "end_time": "2019-02-05_01:31:07",
                "run_time": 0.0011111111111111111,
                "nt": 7284,
                "nsub_stoch": 10,
            },
        },
        2: {
            "sim_name": sim_name_2,
            "EMOD3D": {
                "submit_time": "2019-02-05_01:30:56",
                "cores": 160,
                "start_time": "2019-02-05_01:30:59",
                "end_time": "2019-02-05_01:31:06",
                "run_time": 0.0019444444444444444,
                "nt": 1821,
                "nx": 139,
                "ny": 137,
                "nz": 83,
            },
        },
        3: {
            "sim_name": sim_name_3,
            "HF": {
                "submit_time": "2019-02-05_01:31:07",
                "cores": 80,
                "fd_count": 586,
                "start_time": "2019-02-05_01:31:13",
                "end_time": "2019-02-05_01:31:18",
                "run_time": 0.001388888888888889,
                "nt": 7284,
                "nsub_stoch": 10,
            },
            "IM_calc": {
                "submit_time": "2019-02-05_01:32:29",
                "cores": 40,
                "pSA_count": 15,
                "fd_count": 586,
                "start_time": "2019-02-05_01:32:32",
                "end_time": "2019-02-05_01:32:44",
                "run_time": 0.0033333333333333335,
                "nt": 7284,
                "im_components": ["geom"],
                "im_components_count": 1,
            },
        },
    }

    @pytest.fixture(scope="class")
    def json_files(self, tmpdir_factory):
        tmpdir = tmpdir_factory.getbasetemp()

        # Create the json files
        json_files = []
        for ix in self.json_files_content.keys():
            file = os.path.join(tmpdir, self.test_input_file_template.format(ix))

            with open(file, "w") as f:
                json.dump(self.json_files_content[ix], f)

            json_files.append(file)

        return json_files

    def test_aggreation_single_process(self, json_files):
        df = create_dataframe(json_files, 1, True)
        self.df_check(df)

    def test_aggreation_mp(self, json_files):
        df = create_dataframe(json_files, 3, True)
        self.df_check(df)

    def test_output(self, tmpdir_factory):
        """Tests that an ouput file is written and is correct"""
        tmpdir = tmpdir_factory.getbasetemp()

        output_file = os.path.join(tmpdir, self.test_output_file)
        args = argparse.Namespace(
            input_dirs=[tmpdir],
            output_file=output_file,
            n_procs=1,
            filename_pattern="*",
            calc_core_hours=True,
            not_recursive=False,
        )
        df = main(args)

        # Check that the file exists
        assert os.path.isfile(output_file)

        # Load the saved dataframe
        loaded_df = load_metadata_df(output_file)

        # Check that the dataframes are the same
        pd.testing.assert_frame_equal(df, loaded_df)

    def df_check(self, df: pd.DataFrame):
        """Tests general aggregation of simulation json log files"""
        # Check the shape (this has to updated if the content is changed)
        # Have to add the core hours columns and n_steps for EMOD3D
        assert df.columns.shape[0] == 27 + 3 + 1
        assert df.shape[0] == 3

        # Check that the 3 simulation entries are there
        assert np.all(
            np.isin(
                [self.sim_name_1, self.sim_name_2, self.sim_name_3], df.index.values
            )
        )

        # Check column types
        for col in df.columns:
            # Check date columns are the correct type
            if col[1] in DATE_COLUMNS:
                assert df[col].dtype == np.dtype("<M8[ns]")
            # Check component columns types
            elif const.Components.has_value(col[1]):
                assert df[col].dtype == np.dtype(bool)
            # Check run time and core hours column
            elif col[1] == const.MetadataField.run_time.value:
                assert df[col].dtype == np.dtype(np.float64)
                assert df[
                    col[0], const.MetadataField.core_hours.value
                ].dtype == np.dtype(np.float64)


if __name__ == "__main__":
    pytest.main(sys.argv)
