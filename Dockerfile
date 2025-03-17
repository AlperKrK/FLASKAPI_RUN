#PHYTON
FROM python:3.13.0 

WORKDIR  /phyton-deneme-alper

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt



COPY . .

EXPOSE  5000 


CMD ["python", "alper_deneme.py" ]