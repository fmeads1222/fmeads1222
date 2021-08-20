CREATE WAREHOUSE INTERVIEW_WH
WITH 
WAREHOUSE_SIZE = 'XSMALL'
MAX_CLUSTER_COUNT = 1
MIN_CLUSTER_COUNT = 1
SCALING_POLICY = ECONOMY
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE USER_XXXXX_XXXXX;

CREATE SCHEMA STAGE;
CREATE SCHEMA DW;
