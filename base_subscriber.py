# base_subscriber.py
import redis
import json
import requests
import time
import ssl
from datetime import datetime
import logging
import threading
import os
import dotenv
from typing import Dict, Any
import uuid
from base_publisher import PreprocessingPublisher, DocumentAnalyserPublisher, BusinessRiskReportPublisher
from pub_sub_utils import MessageTracker
# Load environment variables from .env file
dotenv.load_dotenv()

# Configuration
REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
API_ENDPOINT = "http://127.0.0.1:8000"
# FASTAPI_URL = "http://127.0.0.1:8000/"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseSubscriber:
    def __init__(self, redis_client, input_channel: str, output_channel: str, api_endpoint: str):
        self.redis_client = redis_client
        self.input_channel = input_channel
        self.output_channel = output_channel
        self.api_endpoint = api_endpoint
        self.pubsub = self.redis_client.pubsub()
        self.tracker = MessageTracker(redis_client)
    
    def generate_message_id(self) -> str:
        """
        Génère un ID unique pour le message
        """
        return f"msg_{uuid.uuid4().hex[:12]}_{int(datetime.now().timestamp())}"

    def send_to_api(self, task_id: str) -> dict:
        """
        Envoie la requête à l'API et retourne le résultat
        """
        try:
            response = requests.get(f"{self.api_endpoint}/{task_id}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error calling API {self.api_endpoint}: {str(e)}")
            raise

    def publish_to_output(self, result: dict, retry_count: int = 0):
        """
        Publie le résultat dans le canal de sortie
        """
        try:
            message = {
                "id": result.get("id"),
                "task_id": result.get("task_id"),
                "result": result,
                "status": "completed",
                "processed_at": datetime.now().isoformat(),
                "retry_count": retry_count
            }
            self.redis_client.publish(self.output_channel, json.dumps(message))
            logger.info(f"Result published to {self.output_channel} for document {result.get('document_id')}")
            return True
        except Exception as e:
            logger.error(f"Error publishing to output: {str(e)}")
            return False

    def process_message(self, message):
        """
        Traite le message reçu du canal d'entrée
        """
        try:
            data = json.loads(message["data"])
            if not data.get('id'):
                data['id'] = self.generate_message_id()
                
            task_id = data.get('task_id')
            
            if not task_id:
                logger.error("Message missing task_id")
                return

            logger.info(f"Processing task {task_id} for endpoint {self.api_endpoint}")
            
            result = self.send_to_api(task_id)
            result['id'] = data.get('id', str(uuid.uuid4()))
            result['task_id'] = task_id
            
            self.publish_to_output(result)
            
        except Exception as e:
            logger.error(f"Error processing message for {self.api_endpoint}: {str(e)}")

    def start(self):
        """
        Démarre le subscriber
        """
        self.pubsub.subscribe(**{self.input_channel: self.process_message})
        logger.info(f"Subscribed to {self.input_channel} for endpoint {self.api_endpoint}")
        
        while True:
            try:
                self.pubsub.get_message()
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error in main loop for {self.api_endpoint}: {str(e)}")
                time.sleep(1)

# Subscribers spécifiques
class PreprocessingSubscriber(BaseSubscriber):
    def __init__(self, redis_client):
        super().__init__(
            redis_client=redis_client,
            input_channel="preprocessing_input",
            output_channel="preprocessing_output",
            api_endpoint=f"{API_ENDPOINT}/preprocess"
        )

class DocumentAnalyserSubscriber(BaseSubscriber):
    def __init__(self, redis_client):
        super().__init__(
            redis_client=redis_client,
            input_channel="document_analyser_input",
            output_channel="document_analyser_output",
            api_endpoint=f"{API_ENDPOINT}/document_analyser"
        )

class BusinessRiskReportSubscriber(BaseSubscriber):
    def __init__(self, redis_client):
        super().__init__(
            redis_client=redis_client,
            input_channel="business_risk_report_input",
            output_channel="business_risk_report_output",
            api_endpoint=f"{API_ENDPOINT}/business_risk_report"
        )