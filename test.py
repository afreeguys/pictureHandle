import tornado.testing as test
import py_eureka_client.eureka_client as eureka_client
from confluent_kafka import Consumer
from confluent_kafka import TopicPartition
from confluent_kafka import Producer
from data.kafkaListener import KafkaConsume
import json

class MyTestCase(test.AsyncTestCase):
    @test.gen_test
    def test_http_fetch(self):
        eureka_client.init(eureka_server="http://10.18.34.225:8761/eureka/",
                           app_name="SOHOPICHANDLE",
                           instance_port=8887)
        print("eureka inited")
        # res = eureka_client.do_service("AD-GOODSGATHER", "/goods/xpsAuditDPAGoodsService/getGoods",data={},return_type="json")
        # , data = json.dumps({'message': "888888"}).encode("utf-8")
        res = eureka_client.do_service("SOHOPICHANDLE1", "/picture/sycHandle", method="POST", data = json.dumps({'message': "888888"}).encode("utf-8"))
        print("result of other service" + res)


    @test.gen_test
    def test_kafka_consumer(self):
        consume = KafkaConsume()
        consume.run()


