# set base image (host OS)
FROM python:3.9

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . /globant_people_analytics

WORKDIR /globant_people_analytics

CMD ["python", "main.py"]