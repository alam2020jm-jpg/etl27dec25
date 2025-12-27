function_frame
from google.cloud import dataproc_v1
from datetime import datetime
def my_function(cloud_event):
    data=cloud_event.data
    bucket=data.get("bucket")
    file_name=data.get("name")
    project_id="b27dec25"
    region="us-central1"
    main_pyspark_code="gs://mainpysparkjob"
    input_path=f"gs://{bucket}/{file_name}"
    gcs_output=f"gs://processed27dec25"
    bq_table="b27dec25.t27dec25.et27dec25"
    date_of_processed=current_date().strftime("%Y%m%d%h%m%s")
    job_id=f"etl_job_id-{date_of_processed}"
    api_end=f"{region}/googleapi.dataproc:443"
    client=dataproc_v1.BsatchControllerClient(client_options=(apiend_point:api_end))
    job_args=[input_path,gcs_output,bq_table]
    pyspark_batch=dataproc_v1.PySparkBatch("main_python_file_uri"=main_pyspark_code\
                  args=job_args)
    job_config=dataproc_v1.Batch(pyspark_batch=pyspark_batch)
    operation=client.create_batch(
                                parents=f"projects/{project_id}/locations/{region}",
                                batch=job_config,job=job_id)
    




