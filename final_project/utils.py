import logging
# from kafka_logging import KafkaHandler

# kh = KafkaHandler("confluent_kafka.config", "sentiment_logs")
logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO,)
                    # handlers=[kh])