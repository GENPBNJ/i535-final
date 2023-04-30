EXPORT DATA OPTIONS (
  uri = 'gs://bigquery_exports_final_i535/topnewsstories*.csv',
  format = 'CSV',
  overwrite = true,
  header = true,
  field_delimiter=','
) AS

SELECT * FROM `i535-final-project-384623.test.example_data`
TABLESAMPLE SYSTEM ( 10 PERCENT);
