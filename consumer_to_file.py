import argparse
from kafka import KafkaConsumer
import os

# Argümanları alma
parser = argparse.ArgumentParser(description='Kafka Consumer to File')
parser.add_argument('--topic', required=True, help='Kafka topic name')
parser.add_argument('--key', type=int, help='Message key index')
args = parser.parse_args()

# Kafka Consumer oluştur
consumer = KafkaConsumer(
    args.topic,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='group1'
)

# Çıkış dizini
output_dir = '/tmp/kafka_out/'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Dosyalar
setosa_file = open(f"{output_dir}setosa_out.txt", "w")
versicolor_file = open(f"{output_dir}versicolor_out.txt", "w")
virginica_file = open(f"{output_dir}virginica_out.txt", "w")
other_file = open(f"{output_dir}other_out.txt", "w")

# Mesajları tüket
for message in consumer:
    message_value = message.value.decode('utf-8')  # Mesajın içeriği
    message_key = message.key.decode('utf-8') if message.key else None

    # Terminalde mesajı göster
    print(f"Consumed: {message.topic}|{message.partition}|{message.offset}|{message_key}|{message_value}")

    # Mesaj içeriğini ayır ve türüne göre dosyaya yazdır
    if 'Iris-setosa' in message_value:
        setosa_file.write(f"{message.topic}|{message.partition}|{message.offset}|{message_key}|{message_value}\n")
    elif 'Iris-versicolor' in message_value:
        versicolor_file.write(f"{message.topic}|{message.partition}|{message.offset}|{message_key}|{message_value}\n")
    elif 'Iris-virginica' in message_value:
        virginica_file.write(f"{message.topic}|{message.partition}|{message.offset}|{message_key}|{message_value}\n")
    else:
        other_file.write(f"{message.topic}|{message.partition}|{message.offset}|{message_key}|{message_value}\n")

# Dosyaları kapat
setosa_file.close()
versicolor_file.close()
virginica_file.close()
other_file.close()
