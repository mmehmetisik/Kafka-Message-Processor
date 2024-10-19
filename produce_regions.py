import pandas as pd
from kafka import KafkaProducer
import time

# CSV dosyasını okuma
df = pd.read_csv('/root/datasets/tr_il_plaka_kod.csv')

# Kafka producer oluşturma
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Her şehir ve plaka kodunu Kafka'ya gönderme
for index, row in df.iterrows():
    key = bytes(str(row['plaka']), 'utf-8')
    value = bytes(row['il'], 'utf-8')
    producer.send('regions', key=key, value=value)
    print(f"Sent: Key={row['plaka']}, Value={row['il']}")
    time.sleep(0.5)  # Mesajlar arasında bekleme süresi

# Producer'ı kapatma
producer.flush()
producer.close()
