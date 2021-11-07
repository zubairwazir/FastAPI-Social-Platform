FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

COPY ./ ./.

RUN apt-get update -y 
RUN apt-get install python3-pip -y
RUN pip install -r requirements.txt --no-cache-dir
RUN python -m unittest discover --verbose -s ./test/ -p test_*.py

#enviromental variables
ENV PYTHONUNBUFFERED True
ENV WEB_CONCURRENCY 4

# Step 4: Run the web service on container startup using gunicorn webserver.
#CMD exec gunicorn main:app --preload --timeout 300 -k uvicorn.workers.UvicornWorker -w 4 -b :$PORT --threads 5
