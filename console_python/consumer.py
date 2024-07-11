import json
from confluent_kafka import Consumer, KafkaException

def process_msg(msg):
    print('Received message: {}'.format(msg.value().decode('utf-8')))

conf = {'bootstrap.servers': 'localhost:29092', 'group.id': 'grupo2', 'auto.offset.reset': 'earliest'}

consumer = Consumer(conf) #PASAMOS LA CONFIGURACION

#consumer.subscribe(['national-news'])
consumer.subscribe(['international-news'])

ejecutar = True
while ejecutar:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            process_msg(msg)
    except KafkaException:
        pass
    except KeyboardInterrupt:
        ejecutar = False
else:
    print("Coneccion Cerrada")
    consumer.close()

