# syntax=docker/dockerfile:1
FROM python
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .
CMD [ "python3", "-u" , "client.py", "--host=0.0.0.0"]