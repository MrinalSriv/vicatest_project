# vicatest_project

4	Deployment Steps 
Normal Setup
a)	Conda create -n vica python=3.8
b)	Conda activate vica
c)	pip install -r requirements.txt
d)	python main.py

Docker instance
a)	Download the file.
b)	Docker fle details
FROM python:3.8-slim-buster
WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt
CMD [ "python", "./main.py" ] 
c)	docker build -t vica .
d)	docker run --name test2  -v D:\Mrinal\DataPipeline\VICA_pipeline:/app vica
