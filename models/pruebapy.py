import http
import json
import pandas as pd

def call_publicapi(x):

  table_name = str(x)
  status = ''
  execution_log = ''
  try:
    conn = http.client.HTTPSConnection("api.postcodes.io")
    payload = ''
    headers = {
              'Cookie': '__cfduid=d2e270bea97599e2fbde210bf483fcd491615195032'
              }
    for val in range(2):
      conn.request("GET", "/random/postcodes", payload, headers)
      execution_log += 'connection is done'
      res = conn.getresponse()
      data = res.read().decode("utf-8")
      jsondata = json.loads(json.dumps(data))
      execution_log += 'JSON payload is done'
      df = spark.read.json(sc.parallelize([jsondata]))
      if val == 0:
        df_temp = df.selectExpr("string(status) as status","result['country'] as country", "result['european_electoral_region'] as european_electoral_region", "string(result['latitude']) as latitude", "string(result['longitude']) as longitude", "result['parliamentary_constituency'] as parliamentary_constituency", "result['region'] as region","'' as vld_status","'' as vld_status_reason")
        df_union = df_temp
      else:
        df_union = df_union.union(df_temp)
      #df_union.write.format("delta").mode("append").saveAsTable(f"{table_name}")
    #your work
    df22 = table('uk_public_api')
    status = 'success'
    execution_log = f"call_publicapi - success - created successfully"
    x = str(df_temp.select('parliamentary_constituency'))
  except Exception as execution_error:
    status = 'failed'
    execution_log = f"call_publicapi - failed - with error {str(execution_error)}"
    x = f"call_publicapi - failed - with error {str(execution_error)}"
  return df22.select('latitude').filter("parliamentary_constituency == '" + table_name + "'").distinct().toPandas().to_string()


def model(dbt, session):
    dbt.config(
        materialized="table"
    )
    df=dbt.ref("premodelpy")
    pdf=pd.DataFrame()
    pdf = df.toPandas()
    pdf["prueba"] = pdf["CAMPO_PRUEBA"].apply(call_publicapi) 
    return pdf





