FROM python:3.12-slim
WORKDIR /app

RUN pip install \
    fastapi \
    "uvicorn[standard]" \
    "broadcaster[redis]==0.3.1"

COPY ./crates/mmcast/examples/benchmark/redis_side/app.py /app/app.py

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
