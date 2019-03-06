import os
import pickle

from shared_workflow import shared
from testing.test_common_set_up import INPUT, OUTPUT, set_up


def test_get_stations(set_up):
    func_name = 'get_stations'
    for content in set_up:
        for root_path in content:
            with open(
                    os.path.join(root_path, INPUT, func_name + "_source_file.P"), "rb"
            ) as load_file:
                source_file = pickle.load(load_file)
            with open(
                    os.path.join(root_path, INPUT, func_name + "_locations.P"), "rb"
            ) as load_file:
                locations = pickle.load(load_file)

            test_output = shared.get_stations(source_file, locations)

            with open(
                    os.path.join(root_path, OUTPUT, func_name + "_stations.P"), "rb"
            ) as load_file:
                stations = pickle.load(load_file)

            with open(
                    os.path.join(root_path, OUTPUT, func_name + "_station_lats.P"), "rb"
            ) as load_file:
                station_lats = pickle.load(load_file)

            with open(
                    os.path.join(root_path, OUTPUT, func_name + "_station_lons.P"), "rb"
            ) as load_file:
                station_lons = pickle.load(load_file)

            assert test_output == (stations, station_lats, station_lons)


def test_user_select(set_up):
    func_name = 'user_select'
    for content in set_up:
        for root_path in content:
            with open(
                    os.path.join(root_path, INPUT, func_name + "_options.P"), "rb"
            ) as load_file:
                options = pickle.load(load_file)

            test_output = shared.user_select(options)
            with open(
                    os.path.join(root_path, OUTPUT, func_name + "_selected_number.P"), "rb"
            ) as load_file:
                bench_output = pickle.load(load_file)
            assert test_output == bench_output


