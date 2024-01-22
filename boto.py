import os
import json
from typing import Optional

import boto3
from decouple import config
from botocore.config import Config


import logging

logger = logging.getLogger('dev')



class AWSBoto(object):
    AWS_DEFAULT_REGION = config('AWS_DEFAULT_REGION', cast=str)
    AWS_ACCESS_KEY_ID = config('AWS_ACCESS_KEY_ID', cast=str)
    AWS_SECRET_ACCESS_KEY = config('AWS_SECRET_ACCESS_KEY', cast=str)
    BEDROCK_ASSUME_ROLE = config('BEDROCK_ASSUME_ROLE', cast=str)

    def __init__(self, modelId:Optional[str]=None):
        self._client = None
        self._message = None
        self.requests = ""
        self.attempt = 0
        self.logger = logging.getLogger('dev')
        if modelId is not None:
            self.model_id = modelId
        else:
            self.model_id = None

    @property
    def client(self):
        if self._client is None:
            return self.get_bedrock_client()
        return self._client

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, message:str):
        try:
            assert isinstance(message, str)
            self._message = message
        except AssertionError:
            self._message = None

    
    def send(self, text):
        self.add_message(text)
        self._request()


    def _request(self):
        body = json.dumps(
            {"prompt": self.requests, 
            "max_tokens_to_sample": 500,
            "temperature": 0.5,
            "top_k": 250,
            "top_p": 1,
            "stop_sequences": [
            "Human:"
            ],
            "anthropic_version": "bedrock-2023-05-31"
        })
        # modelId = "anthropic.claude-instant-v1"  # change this to use a different version from the model provider
        modelId = self.model_id
        accept = "*/*"
        contentType = "application/json"
        try:
            response = self.client.invoke_model(
                body=body, modelId=modelId, accept=accept, contentType=contentType
            )
        except Exception as err:
            self.attempt += 1
            if self.attempt < 25:
                return self._request()
            else:
                raise
        response_body = json.loads(response.get("body").read())
        # print(response['body'])
        self.message = response_body.get("completion")
        self.add_message(self.message, 'Assistant')

    def add_message(self, text:str, sender: Optional[str] = 'Human', is_human:bool=True):
        self.requests += f"""{sender}: {text}
            {"Assistant:" if sender == 'Human' else ""}
        """

    def get_bedrock_client(self,
        region: Optional[str] = None,
        runtime: Optional[bool] = True,
    ):
        if region is None:
            target_region = self.AWS_DEFAULT_REGION
        else:
            target_region = region

        print(f"Create new client\n  Using region: {target_region}")
        session_kwargs = {
            "region_name": target_region,
            "aws_access_key_id": self.AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": self.AWS_SECRET_ACCESS_KEY
        }
        client_kwargs = {**session_kwargs}

        profile_name = os.environ.get("AWS_PROFILE")
        if profile_name:
            print(f"  Using profile: {profile_name}")
            session_kwargs["profile_name"] = profile_name

        retry_config = Config(
            region_name=target_region,
            retries={
                "max_attempts": 10,
                "mode": "standard",
            },
        )
        session = boto3.Session(**session_kwargs)

        if runtime:
            service_name='bedrock-runtime'
        else:
            service_name='bedrock'

        self._client = session.client(
            service_name=service_name,
            config=retry_config,
            **client_kwargs
        )

        print("boto3 Bedrock client successfully created!")
        print(self._client._endpoint)
        return self._client