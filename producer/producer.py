import math
import os
import socket
from confluent_kafka import Producer
import requests
from typing import Any
from time import sleep
from datetime import datetime, timedelta


def datetime_range(
        start: datetime,
        end: datetime,
        delta: timedelta
) -> datetime:
    current = start
    while current < end:
        yield current
        current += delta


def get_btc_rate_historical(
        start_date: str,
        end_date: str,
        interval: str,
) -> list[dict[str, Any]]:
    dt_start = datetime.strptime(start_date, "%Y-%m-%d")
    dt_end = datetime.strptime(end_date, "%Y-%m-%d")

    number_of_days = (dt_end - dt_start).days

    number_of_steps = math.ceil(number_of_days / 30)

    step = timedelta(days=30)

    results = []

    for i in range(number_of_steps):
        start = dt_start + i * step
        end = start + step

        if end > dt_end:
            end = dt_end

        start_timestamp = int(start.timestamp() * 1000)
        end_timestamp = int(end.timestamp() * 1000)

        url = f"https://api.coincap.io/v2/assets/bitcoin/history?interval={interval}&start={start_timestamp}&end={end_timestamp}"

        response = requests.get(url)
        response.raise_for_status()

        results += response.json()["data"]

    return results


def build_message(
        rate: dict[str, Any]
) -> str:
    kafka_message = f'{rate["date"]},{rate["time"]},{rate["priceUsd"]}'

    return kafka_message


def kafka_callback(
        err,
        msg
) -> None:
    if err is not None:
        print(f"Failed to deliver message: {msg}: {err}")
    else:
        print(f"Message produced: {msg}")


def main() -> None:
    kafka_port = os.environ.get('KAFKA_PORT', "9092")
    kafka_hostname = os.environ.get('KAFKA_HOSTNAME', 'localhost')

    kafka_config = {
        'bootstrap.servers': f"{kafka_hostname}:{kafka_port}",
        'client.id': socket.gethostname()
    }

    producer = Producer(kafka_config)

    btc_rates = get_btc_rate_historical(
        "2023-01-01",
        datetime.now().strftime("%Y-%m-%d"),
        "h1"
    )

    for btc_rate in btc_rates:
        kafka_message = build_message(btc_rate)

        producer.produce('BTC', value=kafka_message, callback=kafka_callback)

        sleep(0.1)

    producer.flush()

    print("Done producing messages")


if __name__ == '__main__':
    main()
