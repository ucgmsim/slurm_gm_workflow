BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS `status_enum` (
	`id`	INTEGER NOT NULL UNIQUE,
	`state`	TEXT NOT NULL UNIQUE,
	PRIMARY KEY(`id`)
);
INSERT OR IGNORE INTO `status_enum` (id,state) VALUES (1,'created');
INSERT OR IGNORE INTO `status_enum` (id,state) VALUES (2,'queued');
INSERT OR IGNORE INTO `status_enum` (id,state) VALUES (3,'running');
INSERT OR IGNORE INTO `status_enum` (id,state) VALUES (4,'unknown');
INSERT OR IGNORE INTO `status_enum` (id,state) VALUES (5,'completed');
INSERT OR IGNORE INTO `status_enum` (id,state) VALUES (6,'failed');
CREATE TABLE IF NOT EXISTS `state` (
	`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	`run_name`	TEXT NOT NULL,
	`proc_type`	INTEGER NOT NULL,
	`status`	INTEGER,
	`job_id`	INTEGER UNIQUE,
	`last_modified`	INTEGER
);
CREATE TABLE IF NOT EXISTS `error` (
    `id`  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    `task_id`INTEGER NOT NULL,
	`error`		TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS `state_search` ON state (status);
CREATE INDEX IF NOT EXISTS `status` ON state (run_name, job_id, status, proc_type);

CREATE TABLE IF NOT EXISTS "proc_type_enum" (
	`id`	INTEGER NOT NULL UNIQUE,
	`proc_type`	TEXT NOT NULL UNIQUE,
	PRIMARY KEY(`id`)
);
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (1,'EMOD3D');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (2,'merge_ts');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (3,'plot_ts');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (4,'HF');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (5,'BB');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (6,'IM_calculation');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (7,'IM_plot');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (8,'rrup');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (9,'Empirical');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (10,'Verification');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (11,'clean_up');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (12,'LF2BB');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (13,'HF2BB');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (14,'plot_srf');
INSERT OR IGNORE INTO `proc_type_enum` (id,proc_type) VALUES (15,'advanced_IM');
CREATE TABLE IF NOT EXISTS `task_time_log` (
	`id`	INTEGER NOT NULL UNIQUE,
	`state_id`	INTEGER,
	`status` INTEGER,
	`time` INTEGER,
	PRIMARY KEY(`id`),
	FOREIGN KEY(`state_id`) REFERENCES state(id)
);
CREATE TABLE IF NOT EXISTS `job_time_log`(
    `id`	INTEGER NOT NULL UNIQUE,
	`job_id`	INTEGER,
	`status` INTEGER,
	`time` INTEGER,
	PRIMARY KEY(`id`),
	FOREIGN KEY(`job_id`) REFERENCES state(job_id)
);
CREATE VIEW IF NOT EXISTS state_view AS
SELECT state.id, state.run_name, proc_type_enum.proc_type, status_enum.state, state.job_id, state.last_modified
FROM state, status_enum, proc_type_enum
WHERE state.proc_type = proc_type_enum.id AND state.status = status_enum.id;
COMMIT;
