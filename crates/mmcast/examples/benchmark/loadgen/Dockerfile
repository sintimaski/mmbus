FROM python:3.12-slim
WORKDIR /app
RUN pip install websockets
COPY ./crates/mmcast/examples/benchmark/loadgen/loadgen.py /app/loadgen.py
ENTRYPOINT ["python", "/app/loadgen.py"]
