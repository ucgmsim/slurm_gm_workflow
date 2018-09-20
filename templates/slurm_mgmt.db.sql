BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "status_enum" (
	`id`	INTEGER NOT NULL UNIQUE,
	`state`	TEXT NOT NULL UNIQUE,
	PRIMARY KEY(`id`)
);
INSERT OR IGNORE INTO `status_enum` (id,state) VALUES (1,'created');
INSERT OR IGNORE  INTO `status_enum` (id,state) VALUES (2,'in-queue');
INSERT OR IGNORE  INTO `status_enum` (id,state) VALUES (3,'running');
INSERT OR IGNORE  INTO `status_enum` (id,state) VALUES (4,'completed');
INSERT OR IGNORE  INTO `status_enum` (id,state) VALUES (5,'failed');
CREATE TABLE IF NOT EXISTS`state` (
	`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	`run_name`	TEXT NOT NULL,
	`proc_type`	INTEGER NOT NULL,
	`status`	INTEGER,
	`job_id`	INTEGER UNIQUE,
	`retries`	INTEGER,
	`error`		TEXT,
	`last_modified`	INTEGER,
	UNIQUE(`run_name`, `proc_type`)
);
CREATE INDEX IF NOT EXISTS state_search ON state (status);
CREATE INDEX IF NOT EXISTS status ON state (run_name, job_id, status, proc_type);

CREATE TABLE IF NOT EXISTS "proc_type_enum" (
	`id`	INTEGER NOT NULL UNIQUE,
	`proc_type`	TEXT NOT NULL UNIQUE,
	PRIMARY KEY(`id`)
);
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (1,'EMOD3D');
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (2,'merge_ts');
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (3,'winbin_aio');
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (4,'HF');
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (5,'BB');
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (6,'IM_calculation');
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (7,'IM_plot');
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (8,'Empirical');
CREATE TABLE IF NOT EXISTS "time_log" (
	`id`	INTEGER NOT NULL UNIQUE,
	`state_id`	INTEGER,
	`type` INTEGER,
	`time` INTEGER,
	PRIMARY KEY(`id`)
);
CREATE VIEW IF NOT EXISTS state_view AS
SELECT state.id, state.run_name, status_enum.state, proc_type_enum.proc_type, state.job_id, state.last_modified
FROM state, status_enum, proc_type_enum
WHERE state.proc_type = proc_type_enum.id AND state.status = status_enum.id 
COMMIT;
