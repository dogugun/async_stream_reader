FROM python:3.10

WORKDIR /Python-dogugun
COPY . /Python-dogugun

ENV STREAM_ENDPOINT http://data-provider:8080/
ENV STREAM_USER sytac
ENV STREAM_PASSWORD 4p9g-Dv7T-u8fe-iz6y-SRW2
EXPOSE 5000

RUN pip3 install --no-cache-dir -r requirements.txt && chmod +x ./runner.sh
CMD ["/bin/bash", "./runner.sh"]
