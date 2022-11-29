import argparse
from pathlib import Path
from sqlite3 import Connection

from qcore import utils, simulation_structure
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

    config = utils.load_yaml(db_task_config)
    config = {
        state: shared_automated_workflow.parse_config_file(tree)
        for state, tree in config.items()
    }
    # for state, tree in raw_config.items():
    #     processed_config[state] = shared_automated_workflow.parse_config_file(tree)

    # I mean really? Couldn't we have just made the MgmtDB a context manager?
    db = MgmtDB.MgmtDB(simulation_structure.get_mgmt_db(cs_root))
    with MgmtDB.connect_db_ctx(db.db_file) as db_cur:
        db_cur: Connection
        db_cur.execute("BEGIN")
        for state, (
            apply_to_all,
            apply_to_pattern,
            apply_to_not_pattern,
        ) in config.items():
            if len(apply_to_all) > 0:
                db_cur.execute(
                    f"UPDATE state SET {db.col_status} = ?, last_modified = strftime('%s','now') "
                    "WHERE proc_type IN ({', '.join('?'*len(apply_to_all))})",
                    (
                        state,
                        apply_to_all,
                    ),
                )
            if len(apply_to_pattern) > 0:
                for pattern, task_set in apply_to_pattern.items:
                    db_cur.execute(
                        f"UPDATE state SET {db.col_status} = ?, last_modified = strftime('%s','now') "
                        "WHERE run_name LIKE ? AND proc_type IN ({', '.join('?'*len(task_set))})",
                        (
                            state,
                            pattern,
                            task_set,
                        ),
                    )
            if len(apply_to_not_pattern) > 0:
                for pattern, task_set in apply_to_not_pattern.items:
                    db_cur.execute(
                        f"UPDATE state SET {db.col_status} = ?, last_modified = strftime('%s','now') "
                        "WHERE run_name NOT LIKE ? AND proc_type IN ({', '.join('?'*len(task_set))})",
                        (
                            state,
                            pattern,
                            task_set,
                        ),
                    )
        db_cur.commit()


if __name__ == "__main__":
    main()
