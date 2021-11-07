# web: bin/run_cloud_sql_proxy &>null && uvicorn main:app --workers 2 --host 0.0.0.0 --port $PORT --lifespan on
#switch to gunicorn #https://stackoverflow.com/questions/59391560/how-to-run-uvicorn-in-heroku
web: bin/run_cloud_sql_proxy &>null && gunicorn main:app --preload --timeout 300 -k uvicorn.workers.UvicornWorker 
data_download: bin/run_cloud_sql_proxy &> null && python batch_jobs/proddatadownload_v2.py
fund_name_string_mapper: bin/run_cloud_sql_proxy &> null && python batch_jobs/fund_name_string_mapper_prod.py
stock_price_downloader: bin/run_cloud_sql_proxy &> null && python batch_jobs/priceDownloader.py
index_downloader: bin/run_cloud_sql_proxy &> null && python batch_jobs/IndexDownloader.py
worldbank_downloader: bin/run_cloud_sql_proxy &> null && python batch_jobs/worldBankUploader.py