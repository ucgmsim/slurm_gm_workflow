"""Unit tests for log_metadata.py, only the storing of metadata logic.
Does not test retrieval of metadata.
"""

import os
import json
import unittest
from typing import Union, Dict

from metadata.log_metadata import store_metadata, LOG_FILENAME, METACONST_TO_ADD
from qcore.constants import ProcessType


class TestLogMetadata(unittest.TestCase):

    test_dir = "./metadata_test_dir"
    test_file = os.path.join(test_dir, LOG_FILENAME)

    def check_values(self, proc_type, values_dict: Dict[str, Union[int, float, str]]):
        with open(self.test_file, "r") as f:
            json_data = json.load(f)

        for k, v in values_dict.items():
            self.assertEqual(json_data[proc_type][k], v)

    @classmethod
    def setUpClass(cls):
        os.makedirs(cls.test_dir)

    def test_new_keys(self):
        """Tests that new keys are added correctly and more than process
        type is handled correctly.
        """
        keys = ["test1", "test2", "test3"]
        values = [22, 22.5, "test3"]
        proc_types = [ProcessType.EMOD3D.str_value, ProcessType.IM_calculation.str_value]

        metadata_values = {key: str(value) for (key, value) in zip(keys, values)}

        # Write all the metadata
        for proc_type in proc_types:
            store_metadata(self.test_file, proc_type, metadata_values)

        # Check
        for proc_type in proc_types:
            self.check_values(
                proc_type, {key: value for (key, value) in zip(keys, values)}
            )

    def test_existing_keys(self):
        """Test adding a key that has already been added once before.
        Expected behaviour is that the primary key value is left the same,
        the existing key is duplicated with as key_1 postfix, and the new key value
        is added as key_2 (etc for additional keys.

        Excpetion is when the key is one of METACONST_TO_ADD, in which case
        the value of the second (and any additional) is added to the primary key value.
        """
        keys_1 = ["test1", "test2", METACONST_TO_ADD[0]]
        values_1 = [22, "start_date_1", 5]

        keys_2 = ["test1", "test2", METACONST_TO_ADD[0]]
        values_2 = [25, "start_date_2", 3]

        keys_3 = ["test1", "test2", METACONST_TO_ADD[0]]
        values_3 = ["test1_str_value", "start_date_3", 7]

        proc_type = ProcessType.EMOD3D.str_value

        # Write the metadata
        for keys, values in [
            (keys_1, values_1),
            (keys_2, values_2),
            (keys_3, values_3),
        ]:
            metadata_values = {key: str(value) for (key, value) in zip(keys, values)}
            store_metadata(self.test_file, ProcessType.EMOD3D.str_value, metadata_values)

        # Check
        self.check_values(
            proc_type,
            {
                "test1": values_1[0],
                "test1_1": values_1[0],
                "test1_2": values_2[0],
                "test1_3": values_3[0],
                "test2": values_1[1],
                "test2_1": values_1[1],
                "test2_2": values_2[1],
                "test2_3": values_3[1],
                METACONST_TO_ADD[0]: values_1[2] + values_2[2] + values_3[2],
                "{}_1".format(METACONST_TO_ADD[0]): values_1[2],
                "{}_2".format(METACONST_TO_ADD[0]): values_2[2],
                "{}_3".format(METACONST_TO_ADD[0]): values_3[2],
            },
        )

    def tearDown(self):
        os.remove(self.test_file)

    @classmethod
    def tearDownClass(cls):
        os.rmdir(cls.test_dir)


if __name__ == "__main__":
    unittest.main()
