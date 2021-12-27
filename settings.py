import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), 'secrets/.env')
load_dotenv(dotenv_path)

BEARER_TOKEN = os.environ.get('BEARER_TOKEN')
BOOTSTRAP_SERVERS = list(os.environ.get('KAFKA_BOOTSTRAP_SERVERS').split())
TOPIC = os.environ.get('KAFKA_TOPIC')
