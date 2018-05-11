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
INSERT OR IGNORE  INTO `status_enum` (id,state) VALUES (6,'wct_limit');
CREATE TABLE IF NOT EXISTS`state` (
	`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	`run_name`	TEXT NOT NULL,
	`proc_type`	INTEGER NOT NULL,
	`status`	INTEGER,
	`job_id`	INTEGER UNIQUE,
	`error`		TEXT,
	UNIQUE(`run_name`, `proc_type`)
);
CREATE TABLE IF NOT EXISTS "proc_type_enum" (
	`id`	INTEGER NOT NULL UNIQUE,
	`proc_type`	TEXT NOT NULL UNIQUE,
	PRIMARY KEY(`id`)
);
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (1,'EMOD3D');
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (2,'post_EMOD3D');
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (3,'HF');
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (4,'BB');
INSERT OR IGNORE  INTO `proc_type_enum` (id,proc_type) VALUES (5,'IM_calculation');
COMMIT;
