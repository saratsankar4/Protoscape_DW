FROM python:3.9.12

EXPOSE 8000

ADD requirements.txt
RUN pip install -r requirements.txt

CMD python main.py
