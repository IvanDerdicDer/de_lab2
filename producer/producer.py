import os
import socket
from confluent_kafka import Producer
import requests
from typing import Any
from time import sleep


def get_btc_rate(
        url: str
) -> dict[str, Any]:
    response = requests.get(url)

    response.raise_for_status()

    response_dict = response.json()

    return response_dict


def build_message(
        rate: dict[str, Any]
) -> str:
    kafka_message = f'{rate["timestamp"]},{rate["data"]["rateUsd"]}'

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

    url = "https://api.coincap.io/v2/rates/bitcoin"

    while True:
        try:
            btc_rate = get_btc_rate(url)
        except requests.exceptions.HTTPError as e:
            print(e)
            continue

        kafka_message = build_message(btc_rate)

        producer.produce('BTC', value=kafka_message, callback=kafka_callback)

        sleep(0.1)
        # producer.flush()


if __name__ == '__main__':
    main()
