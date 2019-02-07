"""
Unit tests for aggregation of metadata from simulation json files.
"""
import os
import shutil
import json
import argparse
import unittest
import numpy as np
import pandas as pd

from metadata.agg_json_data import main, create_dataframe, load_metadata_df, DATE_COLUMNS
import qcore.constants as const


class TestAggJsonData(unittest.TestCase):

    test_dir = "./metadata_test_dir"
    test_output_file = os.path.join(test_dir, "metadata.csv")
    test_input_file_template = os.path.join(test_dir, "log_{}.json")
    json_files = []

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

    @classmethod
    def setUpClass(cls):
        os.makedirs(cls.test_dir)

        # Create the json files
        for ix in cls.json_files_content.keys():
            file = cls.test_input_file_template.format(ix)
            cls.json_files.append(file)

            with open(file, "w") as f:
                json.dump(cls.json_files_content[ix], f)

    def test_aggreation_single_process(self):
        df = create_dataframe(self.json_files, 1, True)
        self.df_check(df)

    def test_aggreation_mp(self):
        df = create_dataframe(self.json_files, 3, True)
        self.df_check(df)

    def test_output(self):
        """Tests that an ouput file is written and is correct"""
        args = argparse.Namespace(
            input_dirs=[self.test_dir],
            output_file=self.test_output_file,
            n_procs=1,
            filename_pattern="*",
            calc_core_hours=True,
            not_recursive=False
        )
        df = main(args)

        # Check that the file exists
        self.assertTrue(os.path.isfile(self.test_output_file))

        # Load the saved dataframe
        loaded_df = load_metadata_df(self.test_output_file)

        # Check that the dataframes are the same
        pd.testing.assert_frame_equal(df, loaded_df)

    def df_check(self, df: pd.DataFrame):
        """Tests general aggregation of simulation json log files"""
        # Check the shape (this has to updated if the content is changed)
        self.assertEqual(
            df.columns.shape[0], 27 + 3
        )  # Have to add the core hours columns
        self.assertEqual(df.shape[0], 3)

        # Check that the 3 simulation entries are there
        self.assertTrue(
            np.all(
                np.isin(
                    [self.sim_name_1, self.sim_name_2, self.sim_name_3], df.index.values
                )
            )
        )

        # Check column types
        for col in df.columns:
            # Check date columns are the correct type
            if col[1] in DATE_COLUMNS:
                self.assertEqual(df[col].dtype, np.dtype("<M8[ns]"))
            # Check component columns types
            elif const.Components.has_value(col[1]):
                self.assertEqual(df[col].dtype, np.dtype(bool))
            # Check run time and core hours column
            elif col[1] == const.MetadataField.run_time.value:
                self.assertEqual(df[col].dtype, np.dtype(np.float))
                self.assertEqual(
                    df[col[0], const.MetadataField.core_hours.value].dtype, np.dtype(np.float)
                )

    def tearDown(self):
        if os.path.isfile(self.test_output_file):
            os.remove(self.test_output_file)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_dir)
