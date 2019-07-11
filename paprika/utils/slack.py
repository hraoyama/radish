from absl import logging
from abc import abstractmethod

from slackclient import SlackClient


class Slack(object):
    @abstractmethod
    def send(self, message):
        pass

    @abstractmethod
    def alert(self, message):
        pass


class SlackLive(Slack):
    def __init__(self):
        self.API_KEY = 'xoxp-155576176225-156934533590-191589206180-d8ef123c411b68d309edf525c929d588'
        self.slack_client = SlackClient(self.API_KEY)

    def send(self, message):
        self._send(message, '#models')

    def alert(self, message):
        self._send(message, '#alerts')

    def _send(self, message, channel):
        try:
            self.slack_client.api_call(
                'chat.postMessage',
                channel=channel,
                text=message
            )
        except Exception as e:
            logging.error(
                'Failed to send message: %s. Got exception: %s',
                message,
                str(e))


class SlackOffline(Slack):
    def send(self, message):
        pass

    def alert(self, message):
        pass
