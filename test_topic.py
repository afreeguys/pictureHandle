import tornado.testing as test
import py_eureka_client.eureka_client as eureka_client
from confluent_kafka import Consumer
from confluent_kafka import TopicPartition
from confluent_kafka import Producer
import time
import json

class MyTestCase(test.AsyncTestCase):

    @test.gen_test
    def test_kafka_consumer1(self):
        consumer = Consumer({
            'bootstrap.servers': 'kfk013223.heracles.sohuno.com:9092,kfk013221.heracles.sohuno.com:9092,pdn096224.heracles.sohuno.com:9092,pdn096223.heracles.sohuno.com:9092,kfk013225.heracles.sohuno.com:9092,kfk013224.heracles.sohuno.com:9092,kfk013222.heracles.sohuno.com:9092,kfk013216.heracles.sohuno.com:9092,kfk013215.heracles.sohuno.com:9092,kfk131132.heracles.sohuno.com:9092,kfk131133.heracles.sohuno.com:9092,kfk131131.heracles.sohuno.com:9092,kfk013219.heracles.sohuno.com:9092,pdn096227.heracles.sohuno.com:9092,kfk013220.heracles.sohuno.com:9092,kfk131134.heracles.sohuno.com:9092,kfk013218.heracles.sohuno.com:9092,kfk013217.heracles.sohuno.com:9092,pdn096229.heracles.sohuno.com:9092,pdn096228.heracles.sohuno.com:9092,pdn096226.heracles.sohuno.com:9092,pdn096230.heracles.sohuno.com:9092,pdn096225.heracles.sohuno.com:9092,pdn096222.heracles.sohuno.com:9092,pdn096221.heracles.sohuno.com:9092',
            'group.id': 'mygroup',
            'auto.offset.reset': 'earliest'
        })

        # eureka_client.init(eureka_server="http://10.18.34.225:8761/eureka/",
        #                    app_name="SOHOPICHANDLE")
        # print("eureka inited")
        consumer.subscribe(["mbads_dpa_goods_pic_test"])

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("0-Consumer error: {}".format(msg.error()))
                continue
            print('0-Received message: {}'.format(msg.value().decode('utf-8')))
            # res = eureka_client.do_service("SOHOPICHANDLE1", "/picture/sycHandle", method="POST", data = msg.value())
            # print('0-result: {}'.format(res))
            # consumer.commit(msg)
            # consumer.resume([TopicPartition("mbads_dpa_goods_pic_test", 0)])


    @test.gen_test
    def test_kafka_consumer2(self):
        consumer = Consumer({
            'bootstrap.servers': 'kfk013223.heracles.sohuno.com:9092,kfk013221.heracles.sohuno.com:9092,pdn096224.heracles.sohuno.com:9092,pdn096223.heracles.sohuno.com:9092,kfk013225.heracles.sohuno.com:9092,kfk013224.heracles.sohuno.com:9092,kfk013222.heracles.sohuno.com:9092,kfk013216.heracles.sohuno.com:9092,kfk013215.heracles.sohuno.com:9092,kfk131132.heracles.sohuno.com:9092,kfk131133.heracles.sohuno.com:9092,kfk131131.heracles.sohuno.com:9092,kfk013219.heracles.sohuno.com:9092,pdn096227.heracles.sohuno.com:9092,kfk013220.heracles.sohuno.com:9092,kfk131134.heracles.sohuno.com:9092,kfk013218.heracles.sohuno.com:9092,kfk013217.heracles.sohuno.com:9092,pdn096229.heracles.sohuno.com:9092,pdn096228.heracles.sohuno.com:9092,pdn096226.heracles.sohuno.com:9092,pdn096230.heracles.sohuno.com:9092,pdn096225.heracles.sohuno.com:9092,pdn096222.heracles.sohuno.com:9092,pdn096221.heracles.sohuno.com:9092',
            'group.id': 'mygroup',
            'auto.offset.reset': 'earliest'
        })

        consumer.subscribe(["mbads_dpa_goods_pic_test"])
        # time.sleep(1)
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("1-Consumer error: {}".format(msg.error()))
                continue
            print('1-Received message: {}'.format(msg.value().decode('utf-8')))
            # consumer.commit(msg)
            # consumer.resume([TopicPartition("mbads_dpa_goods_pic_test", 0)])

    @test.gen_test
    def test_kafka_produce(self):
        producer = Producer({
            'bootstrap.servers': 'kfk013223.heracles.sohuno.com:9092,kfk013221.heracles.sohuno.com:9092,pdn096224.heracles.sohuno.com:9092,pdn096223.heracles.sohuno.com:9092,kfk013225.heracles.sohuno.com:9092,kfk013224.heracles.sohuno.com:9092,kfk013222.heracles.sohuno.com:9092,kfk013216.heracles.sohuno.com:9092,kfk013215.heracles.sohuno.com:9092,kfk131132.heracles.sohuno.com:9092,kfk131133.heracles.sohuno.com:9092,kfk131131.heracles.sohuno.com:9092,kfk013219.heracles.sohuno.com:9092,pdn096227.heracles.sohuno.com:9092,kfk013220.heracles.sohuno.com:9092,kfk131134.heracles.sohuno.com:9092,kfk013218.heracles.sohuno.com:9092,kfk013217.heracles.sohuno.com:9092,pdn096229.heracles.sohuno.com:9092,pdn096228.heracles.sohuno.com:9092,pdn096226.heracles.sohuno.com:9092,pdn096230.heracles.sohuno.com:9092,pdn096225.heracles.sohuno.com:9092,pdn096222.heracles.sohuno.com:9092,pdn096221.heracles.sohuno.com:9092',
        })
        for i in range(200):
            producer.produce(topic="mbads_dpa_goods_pic_test",value=json.dumps({'message': "888888",'index':i}))

        # for i in range(10):
        #     producer.produce(topic="mbads_dpa_goods_pic_test",partition=0,value="11111111111111111111")
        producer.flush(2)
