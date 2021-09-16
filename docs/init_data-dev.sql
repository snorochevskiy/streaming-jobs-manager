INSERT INTO EMR_APPS (ID, NAME, DESCRIPTION) VALUES ('myapp-1-dev', 'MyApp 1 DEV', '');

INSERT INTO EMR_APP_IDENTIFIERS
  (ID, EMR_APP_ID, NAME, AWS_REGION, LOOKUP_TYPE, VERSION, PARAM1, PARAM2)
  VALUES
  ('myapp-1-dev-by_name', 'myapp-1-dev', '', 'us-east-1', 'emr_name_regex', NULL, '((?:east)|(?:west))-(dev)-(?<version>[-0-9.a-zA-Z]+)\\.(myapp.1)\\.(.*)', NULL);

INSERT INTO EMR_APP_CW_METRICS
  (ID, EMR_APP_ID, NAME, AWS_REGION, NAME_SPACE, METRIC_NAME, AGG_FUNC, SECONDS_TO_NOW, CHECK_TYPE, VALUE_COMPARE_AGAINST)
  VALUES
  ('myapp-1-ingesting_data', 'myapp-1-dev', 'Ingesting data', 'us-east-1', 'myapp-1-dev', 'batchInputRowCount', 'sum', 3600, 'greater', 0.0),
  ('myapp-1-producing_data', 'myapp-1-dev', 'Producing data', 'us-east-1', 'myapp-1-dev', 'sentMessages', 'sum', 3600, 'greater', 0.0)
;
