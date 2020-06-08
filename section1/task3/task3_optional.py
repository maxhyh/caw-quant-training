from crypto_news_api import CryptoControlAPI
import json

class cryptonews():

    def __init__(self, key):
        self.key = key
        self.api = CryptoControlAPI(key)
        self.proxyApi = CryptoControlAPI(key, "http://cryptocontrol_proxy/api/v1/public")

    def enableSentiment(self):
        self.sentiment = True

    def top_news(self, lang = None):
        return self.api.getTopNews(language= lang)

    def news_by_coin(self, coin, lang=None):
        return self.api.getTopNewsByCoin(coin=coin, language=lang)

    def get_top_tweets(self, coin, lang=None):
        return self.api.getTopTweetsByCoin(coin=coin, language=lang)

    def get_reddit(self, coin, lang=None):
        return self.api.getLatestRedditPostsByCoin(coin=coin, language=lang)
    
    def get_top_feed(self, coin, lang):
        return  self.api.getTopFeedByCoin(coin=coin, language=lang)

    def get_latest(self, coin, lang):
        return  self.api.getLatestItemsByCoin(coin=coin, language=lang)

    def get_coinDetails(self, coin, lang):
        return  self.api.getCoinDetails(coin=coin, language=lang)

with open('/Users/yihuihuang/Desktop/Crypto/caw-quant-training/section1/task3/api_key_news.json', mode='r') as key_file:
    key = json.loads(key_file.read())['key']

new = cryptonews(key=key)
new = new.news_by_coin('bitcoin')

with open('/Users/yihuihuang/Desktop/Crypto/caw-quant-training/section1/task3/news.json', 'w') as outfile:
    json.dump(new, outfile, indent= 4)
