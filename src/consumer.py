from confluent_kafka import Consumer, KafkaError, KafkaException, Message
import sys


def msg_process(msg: Message):
    print(msg.value())


def basic_consume_loop(
        consumer,
        topics
) -> None:
    try:
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def main() -> None:
    kafka_config = {
        'bootstrap.servers': "localhost:9092",
        'group.id': "foo",
        'auto.offset.reset': 'smallest'
    }

    consumer = Consumer(kafka_config)
    basic_consume_loop(consumer, ["BTC"])


if __name__ == '__main__':
    main()
