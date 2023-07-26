#! python3
"""
Sets a cybershake database to a state determined by the db_task_config file.

The format of the db_task_config file is as follows:
<database state>:
    <process name>:
        <SQL style patterns to match>
Where each of "process name" and "SQL" are repeatable if there are multiple things of the parent that need to be set
No database state can be repeated
"""
import argparse
from pathlib import Path
from typing import Dict, Union, List

from qcore import utils, simulation_structure, constants
from workflow.automation.lib import shared_automated_workflow, MgmtDB


def load_args():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("db_task_config", type=Path)
    parser.add_argument("--cybershake_root", default=Path("."), type=Path)
    args = parser.parse_args()
    return args


def main():
    args = load_args()
    db_task_config = args.db_task_config.absolute()
    cs_root = args.cybershake_root.absolute()

    # Load config
    config: Dict[str, Dict[str, Union[str, List[str]]]] = utils.load_yaml(
        db_task_config
    )
    errors = []
    for state in config.keys():
        if not constants.Status.has_str_value(state):
            errors.append(
                f"State {state} in db_task_config not valid. Valid states are {list(constants.Status.iterate_str_values())}."
            )
    if errors:
        raise ValueError(
            f"Error(s) were found, please correct these before re-running: {', '.join(errors)}"
        )

    config = {
        constants.Status.from_str(state): shared_automated_workflow.parse_config_file(
            tree
        )
        for state, tree in config.items()
    }

    db = MgmtDB.MgmtDB(simulation_structure.get_mgmt_db(cs_root))
    with MgmtDB.connect_db_ctx(db.db_file, verbose=True) as db_cur:
        db_cur.execute("BEGIN")
        for (
            state,
            (apply_to_all, apply_to_pattern, apply_to_not_pattern,),
        ) in config.items():
            if len(apply_to_all) > 0:
                db_cur.execute(
                    f"UPDATE state SET {db.col_status} = ?, last_modified = strftime('%s','now') "
                    f"WHERE proc_type IN ({', '.join('?'*len(apply_to_all))})",
                    (state.value, *[x.value for x in apply_to_all],),
                )
            if len(apply_to_pattern) > 0:
                for pattern, task_set in apply_to_pattern:
                    db_cur.execute(
                        f"UPDATE state SET {db.col_status} = ?, last_modified = strftime('%s','now') "
                        f"WHERE run_name LIKE ? AND proc_type IN ({', '.join('?'*len(task_set))})",
                        (state.value, pattern, *[x.value for x in task_set],),
                    )
            if len(apply_to_not_pattern) > 0:
                for pattern, task_set in apply_to_not_pattern:
                    db_cur.execute(
                        f"UPDATE state SET {db.col_status} = ?, last_modified = strftime('%s','now') "
                        f"WHERE run_name NOT LIKE ? AND proc_type IN ({', '.join('?'*len(task_set))})",
                        (state.value, pattern, *[x.value for x in task_set],),
                    )


if __name__ == "__main__":
    main()
