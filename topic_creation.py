from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

def create_a_new_topic_if_not_exists(admin_client, topic_name="example-topic", num_partitions=1, replication_factor=1):
    try:
        # Tüm mevcut topic'leri getir
        existing_topics = admin_client.list_topics()

        # Eğer topic hali hazırda varsa hiçbir şey yapma
        if topic_name in existing_topics:
            print(f"Topic '{topic_name}' already exists.")
            return

        # Eğer topic yoksa yeni bir topic oluştur
        new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' has been created successfully.")

    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    except KafkaError as e:
        print(f"Failed to create topic '{topic_name}': {e}")

if __name__ == "__main__":
    admin_client = None
    try:
        # KafkaAdminClient'ı oluştur
        admin_client = KafkaAdminClient(
            bootstrap_servers=["127.0.0.1:9092", "127.0.0.1:9292", "127.0.0.1:9392"],  # Birden fazla Kafka broker'ın adresi
            client_id='test_admin'
        )

        # Fonksiyonu çağır
        create_a_new_topic_if_not_exists(admin_client, "new_topic_example", num_partitions=3, replication_factor=1)

    except KafkaError as e:
        print(f"Failed to connect to Kafka: {e}")
    finally:
        # admin_client tanımlıysa bağlantıyı kapat
        if admin_client is not None:
            admin_client.close()