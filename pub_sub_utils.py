import json
import logging
from datetime import datetime
from typing import Any, Dict
import requests

logger = logging.getLogger(__name__)

class MessageTracker:
    def __init__(self, redis_client, processing_hash_key="processing_messages"):
        self.redis_client = redis_client
        self.processing_hash_key = processing_hash_key

    def add_pending_message(self, message: Dict[str, Any]):
        """
        Ajoute un message à la hash des messages en cours
        """
        try:
            message_id = str(message.get('id', ''))
            if not message_id:
                logger.error("Message has no ID")
                return

            # S'assurer que tout le message est sérialisable
            message_data = {
                'message': {
                    'id': message_id,
                    'task_id': message.get('task_id'),
                    'result': message.get('result'),
                    'status': message.get('status', 'pending')
                },
                'status': 'pending',
                'added_at': datetime.now().isoformat(),
                'retry_count': 0
            }

            # Convertir en JSON avant de stocker
            serialized_data = json.dumps(message_data)

            self.redis_client.hset(
                name=self.processing_hash_key,
                key=message_id,
                value=serialized_data
            )
            logger.info(f"Added message {message_id} to processing queue")

        except Exception as e:
            logger.error(f"Error adding message to tracker: {str(e)}")
            logger.error(f"Message content: {message}")

    def update_message_status(self, message_id: str, status: str, error: str = None):
        """
        Met à jour le statut d'un message existant
        """
        try:
            if not message_id:
                logger.error("No message_id provided for update")
                return

            # Récupérer le message existant
            existing_data = self.redis_client.hget(self.processing_hash_key, str(message_id))
            
            if existing_data:
                try:
                    data = json.loads(existing_data)
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON in existing data: {existing_data}")
                    data = {}

                # Mettre à jour les champs
                data['status'] = status
                data['updated_at'] = datetime.now().isoformat()

                data['retry_count'] = data.get('retry_count', 0) + 1

                if error:
                    data['error'] = error

                # Convertir en JSON et stocker
                serialized_data = json.dumps(data)
                # logger.info(f"Updating message with data: {serialized_data}")

                self.redis_client.hset(
                    name=self.processing_hash_key,
                    key=str(message_id),
                    value=serialized_data
                )
                logger.info(f"Updated message {message_id} status to {status}")
            else:
                logger.warning(f"Message {message_id} not found in processing queue")

        except Exception as e:
            logger.error(f"Error updating message status: {str(e)}")
            logger.error(f"Message ID: {message_id}, Status: {status}, Error: {error}")

    def remove_message(self, message_id: str):
        """
        Supprime un message de la hash
        """
        try:
            if not message_id:
                logger.error("No message_id provided for removal")
                return

            result = self.redis_client.hdel(self.processing_hash_key, str(message_id))
            if result:
                logger.info(f"Removed message {message_id} from processing queue")
            else:
                logger.warning(f"Message {message_id} not found in processing queue")

        except Exception as e:
            logger.error(f"Error removing message: {str(e)}")

class WebhookSender:
    def __init__(self, webhook_url: str, timeout: int = 10):
        self.webhook_url = webhook_url
        self.timeout = timeout

    def send_to_frontend(self, message: Dict[str, Any]) -> bool:
        """
        Envoie le message au frontend via webhook
        
        Args:
            message: Le message à envoyer
            
        Returns:
            bool: True si l'envoi a réussi, False sinon
        """
        try:
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'PreprocessingService/1.0'
            }

            response = requests.post(
                url=self.webhook_url,
                json=message,
                headers=headers,
                timeout=self.timeout
            )

            if response.status_code in [200, 201, 202]:
                logger.info(f"Message {message.get('id')} successfully sent to webhook")
                return True
            else:
                logger.error(f"Webhook returned status code {response.status_code} for message {message.get('id')}")
                return False

        except requests.exceptions.Timeout:
            logger.error(f"Timeout sending message {message.get('id')} to webhook")
            return False
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error sending message {message.get('id')} to webhook")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message {message.get('id')} to webhook: {str(e)}")
            return False