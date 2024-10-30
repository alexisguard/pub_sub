import os
import json
import time
import logging
import dotenv
import requests
from typing import Dict, Any
from datetime import datetime
from pub_sub_utils import MessageTracker, WebhookSender

dotenv.load_dotenv()
logger = logging.getLogger(__name__)


class BaseOutputPublisher:
    def __init__(self, redis_client, output_channel: str, webhook_url: str):
        self.redis_client = redis_client
        self.output_channel = output_channel
        self.pubsub = self.redis_client.pubsub()
        self.retry_delays = [5, 15, 30, 60, 120]
        self.webhook_sender = WebhookSender(webhook_url)
        self.tracker = MessageTracker(redis_client)
        logger.info(f"Output publisher initialized for channel {output_channel} with webhook URL: {webhook_url}")

    def handle_retry(self, message: dict, retry_count: int):
        """
        Gère les retries en cas d'échec
        """
        message_id = message.get('id')
        if not message_id:
            logger.error("Cannot retry message without ID")
            return

        if retry_count >= len(self.retry_delays):
            logger.error(f"Max retries reached for message {message_id}")
            self.tracker.update_message_status(
                message_id,
                'max_retries_reached',
                f'Failed after {retry_count} attempts'
            )
            return

        try:
            new_retry_count = retry_count + 1
            delay = self.retry_delays[retry_count]

            message["retry_count"] = new_retry_count
            
            self.tracker.update_message_status(
                message_id,
                'pending_retry',
                f'Retry attempt {new_retry_count}/{len(self.retry_delays)}',
            )

            logger.info(f"Scheduling retry {new_retry_count} for message {message_id} in {delay} seconds")
            
            time.sleep(delay)
            
            self.redis_client.publish(self.output_channel, json.dumps(message))
            logger.info(f"Message {message_id} requeued for retry {new_retry_count}")

        except Exception as e:
            logger.error(f"Error handling retry for message {message_id}: {str(e)}")
            self.tracker.update_message_status(
                message_id,
                'retry_error',
                str(e)
            )

    def process_output_message(self, message):
        """
        Traite les messages de la queue de sortie
        """
        try:
            data = json.loads(message["data"])
            message_id = data.get('id')
            
            if not message_id:
                logger.error("Message has no ID")
                return
                
            retry_count = data.get("retry_count", 0)
            
            self.tracker.add_pending_message(data)
            
            logger.info(f"Processing message {message_id} (retry {retry_count}) on channel {self.output_channel}")
            
            success = self.webhook_sender.send_to_frontend(data)

            if not success:
                logger.warning(f"Failed to send message {message_id} (retry {retry_count})")
                self.tracker.update_message_status(
                    message_id,
                    'failed',
                    f'Webhook delivery failed on attempt {retry_count + 1}',
                )
                self.handle_retry(data, retry_count)
            else:
                logger.info(f"Successfully sent message {message_id} after {retry_count} retries")
                self.tracker.remove_message(message_id)

        except Exception as e:
            if message_id:
                self.tracker.update_message_status(
                    message_id,
                    'error',
                    str(e)
                )
            logger.error(f"Error processing output message: {str(e)}")

    def start(self):
        """
        Démarre le publisher
        """
        self.pubsub.subscribe(**{self.output_channel: self.process_output_message})
        logger.info(f"Subscribed to {self.output_channel}")
        
        while True:
            try:
                self.pubsub.get_message()
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error in publisher loop: {str(e)}")
                time.sleep(1)

# Publishers spécifiques
class PreprocessingPublisher(BaseOutputPublisher):
    def __init__(self, redis_client):
        super().__init__(
            redis_client=redis_client,
            output_channel="preprocessing_output",
            webhook_url=os.getenv("PREPROCESSING_WEBHOOK_URL")
        )

class DocumentAnalyserPublisher(BaseOutputPublisher):
    def __init__(self, redis_client):
        super().__init__(
            redis_client=redis_client,
            output_channel="document_analyser_output",
            webhook_url=os.getenv("DOCUMENT_ANALYSER_WEBHOOK_URL")
        )

class BusinessRiskReportPublisher(BaseOutputPublisher):
    def __init__(self, redis_client):
        super().__init__(
            redis_client=redis_client,
            output_channel="business_risk_report_output",
            webhook_url=os.getenv("BUSINESS_RISK_WEBHOOK_URL")
        )