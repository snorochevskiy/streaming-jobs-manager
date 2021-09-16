-- !Ups

create table EMR_APPS (
    ID VARCHAR(128) NOT NULL PRIMARY KEY,
    NAME VARCHAR(256) UNIQUE NOT NULL,
    DESCRIPTION VARCHAR(4096)
);

create table EMR_APP_IDENTIFIERS (
    ID  VARCHAR(128) NOT NULL PRIMARY KEY,
    EMR_APP_ID VARCHAR(128),
    NAME VARCHAR(256),
    AWS_REGION VARCHAR(32) NOT NULL,
    LOOKUP_TYPE VARCHAR(32) NOT NULL,
    VERSION VARCHAR(32),
    PARAM1 VARCHAR(4096),
    PARAM2 VARCHAR(4096),
    FOREIGN KEY (EMR_APP_ID) REFERENCES EMR_APPS (ID) ON DELETE CASCADE
);

create table EMR_APP_CW_METRICS (
    ID VARCHAR(128) NOT NULL PRIMARY KEY,
    EMR_APP_ID VARCHAR(128),
    NAME VARCHAR(256),
    AWS_REGION VARCHAR(32) NOT NULL,
    NAME_SPACE VARCHAR(255) NOT NULL,
    METRIC_NAME VARCHAR(255) NOT NULL,
    AGG_FUNC VARCHAR(32) NOT NULL,
    SECONDS_TO_NOW INTEGER DEFAULT 3600,
    CHECK_TYPE VARCHAR(32) NOT NULL,
    VALUE_COMPARE_AGAINST FLOAT,
    FOREIGN KEY (EMR_APP_ID) REFERENCES EMR_APPS (ID) ON DELETE CASCADE
);

create table EMR_APP_RESTART_LOGS (
    ID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
    EMR_APP_ID VARCHAR(128),
    CLUSTER_ID VARCHAR(32),
    REDEPLOY_TIMESTAMP TIMESTAMP,
    FAILED_STEP_START TIMESTAMP,
    FAILED_STEP_END TIMESTAMP,
    REASON VARCHAR(1024)
);

create table EMR_APP_HEALTHCHECK_FAIL_LOGS (
    ID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
    EMR_APP_ID VARCHAR(128),
    CLUSTER_ID VARCHAR(32),
    DETECT_TIMESTAMP TIMESTAMP,
    HC_NAME VARCHAR(128),
    HC_LABEL VARCHAR(256)
);

-- !Downs

drop table EMR_APP_HEALTHCHECK_FAIL_LOGS;
drop table EMR_APP_RESTART_LOGS;
drop table EMR_APP_CW_METRICS;
drop table EMR_APP_IDENTIFIERS;
drop table EMR_APPS;