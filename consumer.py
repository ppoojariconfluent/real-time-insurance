from confluent_kafka import Consumer, KafkaError
import json

conf_consumer = {
     'bootstrap.servers': '',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '',
    'sasl.password': '',
    'group.id': 'final_insurance_consumer',
    'auto.offset.reset': 'latest'
}

def consume_final_insurance_quotes():
    c = Consumer(conf_consumer)

    c.subscribe(['premium_quote'])

    try:
        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value())
            owner_name = data['OWNER_NAME']
            car_model = data['CAR_MODEL']
            idv = data['IDV']
            premium = data['PREMIUM']
            

            print(f"Owner Name: {owner_name}")
            print(f"Car Model: {car_model}")
            print(f"IDV: {idv}")
            print(f"Premium: {premium}")
           

    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == "__main__":
    consume_final_insurance_quotes()
