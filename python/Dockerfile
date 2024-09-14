FROM python:alpine
ADD . .app
WORKDIR /app
COPY . .
RUN pip3 install -r requirements.txt
CMD ["gunicorn", "src.index:app"]
