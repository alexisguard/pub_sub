from base_publisher import PreprocessingPublisher, DocumentAnalyserPublisher, BusinessRiskReportPublisher
from base_subscriber import PreprocessingSubscriber, DocumentAnalyserSubscriber, BusinessRiskReportSubscriber
import threading
import logging
import redis
import ssl
import logging
import dotenv
import os

# Charger les variables d'environnement
dotenv.load_dotenv()

# Configuration du logger
logger = logging.getLogger(__name__)

# Configuration de la connexion à Redis
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

def main():
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        ssl=True,
        ssl_cert_reqs=ssl.CERT_NONE,
        decode_responses=True
    )

    # Créer les subscribers et publishers
    subscribers = [
        PreprocessingSubscriber(redis_client),
        DocumentAnalyserSubscriber(redis_client),
        BusinessRiskReportSubscriber(redis_client)
    ]

    publishers = [
        PreprocessingPublisher(redis_client),
        DocumentAnalyserPublisher(redis_client),
        BusinessRiskReportPublisher(redis_client)
    ]

    threads = []

    # Démarrer les subscribers
    for subscriber in subscribers:
        thread = threading.Thread(
            target=subscriber.start,
            name=f"{subscriber.__class__.__name__}_thread"
        )
        threads.append(thread)
        thread.start()
        logger.info(f"Started {thread.name}")

    # Démarrer les publishers
    for publisher in publishers:
        thread = threading.Thread(
            target=publisher.start,
            name=f"{publisher.__class__.__name__}_thread"
        )
        threads.append(thread)
        thread.start()
        logger.info(f"Started {thread.name}")

    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        logger.info("Shutting down system...")

if __name__ == "__main__":
    main()