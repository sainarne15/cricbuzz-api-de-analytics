from googleapiclient.discovery import build


def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "original-voyage-429415-f7"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bq-load",  # Provide a unique name for the job
        "parameters": {
        "javascriptTextTransformGcsPath": "gs://ckt-rank-data/dataflow-metadata/udf.js",
        "JSONPath": "gs://ckt-rank-data/dataflow-metadata/bq.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "original-voyage-429415-f7:ckt_de_analytics.odi_batsmen_ranking
",
        "inputFilePattern": "gs://ckt-rank-data/batsmen_rankings.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://ckt-rank-data/temp/",
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)