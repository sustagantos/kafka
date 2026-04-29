import json
import uuid

from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)    #where the events produced are going to be sent

def delivery_report(err,msg):
    if err:
        print(f"Delivery error: {err}")
    else:
        print(f"Delivered {msg.value().decode("utf-8")}")
        print(f"Delived to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")
        #print(dir(msg))

order = {
    "order_id": str(uuid.uuid4()),
    "user": "julia",
    "item": "frozen yogurt",
    "quantity": 10
}

value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report    
)

producer.flush()