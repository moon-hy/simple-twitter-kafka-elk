from kafka import KafkaProducer
import requests
import json
from time import sleep
import settings


class FilteredStreamAPI():
    def __init__(self):
        if not settings.BEARER_TOKEN:
            raise Exception(f'Bearer Token not exists.')

        self.rules = self.__get_rules()
        self.rules = self.__delete_all_rules()
        self.sample_rules = [{"value": "-ةُ lang:ko", "tag": "korean"}]
        self.sample_params = {"tweet.fields":"created_at,lang,in_reply_to_user_id,referenced_tweets,entities,source"}
        self.__set_rules()

        self.producer = KafkaProducer(
            bootstrap_servers = settings.BOOTSTRAP_SERVERS,
            acks = 1,
            value_serializer = lambda x: json.dumps(x, indent=4, sort_keys=True).encode('utf-8')
        )

    def bearer_oauth(self, r):
        r.headers["Authorization"] = f"Bearer {settings.BOOTSTRAP_SERVERS}"
        return r

    def __get_rules(self):
        url = "https://api.twitter.com/2/tweets/search/stream/rules"
        response = requests.get(url, auth=self.bearer_oauth)

        if response.status_code != 200:
            raise Exception(
                f'Cannot get rules (HTTP {response.status_code}: {response.text})'
                )
        return response.json()

    def __delete_all_rules(self):
        url = 'https://api.twitter.com/2/tweets/search/stream/rules'
        if self.rules is None or 'data' not in self.rules:
            return None

        ids = list(map(lambda rule: rule['id'], self.rules['data']))
        payload = {'delete': {'ids': ids}}
        response = requests.post(
            url,
            auth=self.bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                f'Cannot delete rules (HTTP {response.status_code}): {response.text}'
                )
        print(json.dumps(response.json()))

    def __set_rules(self):
        url = 'https://api.twitter.com/2/tweets/search/stream/rules'
        # You can adjust the rules if needed
        
        print(f'Add rules with sample_rules:\n{self.sample_rules}')
        payload = {'add': self.sample_rules}
        response = requests.post(
            url,
            auth=self.bearer_oauth,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                f'Cannot add rules (HTTP {response.status_code}): {response.text}'
                )
                
        sleep(3)
        
    def run(self):
        url = 'https://api.twitter.com/2/tweets/search/stream'
        
        print(f'Run with sample_params:\n{self.sample_params}')
        response = requests.get(
            url, auth=self.bearer_oauth, params=self.sample_params, stream=True,
        )

        if response.status_code != 200:
            raise Exception(
                f'Cannot get stream (HTTP {response.status_code}): {response.text}'
            )
        print(f'Response code: {response.status_code}')

        cnt = 0
        for response_line in response.iter_lines():
            if cnt % 100==0:
                print(f'# of response_line: {cnt}')
            if response_line:
                json_response = json.loads(response_line)
                self.producer.send("tweetstream-ko", json_response)
                cnt+=1
