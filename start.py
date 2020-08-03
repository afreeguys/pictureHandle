import logging
from datetime import date
import tornado.ioloop
import tornado.web
import tornado.routing as routing
import py_eureka_client.eureka_client as eureka_client
from controller import picture
from confluent_kafka import Consumer


FORMAT = '%(asctime)-15s  %(message)s'
current = date.today()
logging.basicConfig(filename="D:\opt\logs\picHandle\picHandl" + current.__str__() + ".log", format=FORMAT, level="INFO")
logger = logging.getLogger("picHandle")
REST_SERVER_PORT = 8888
kafkaConsumer = None

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write({"data":1234567})

def initServer():
    eureka_client.init(eureka_server="http://10.18.34.225:8761/eureka/",
                       app_name="SOHOPICHANDLE1",
                       instance_port=REST_SERVER_PORT)

    picHandle = tornado.web.Application([
        (r"/picture/asycHandle", picture.AsycHandle),
        (r"/picture/sycHandle", picture.SycHandle),
    ])

    router = routing.RuleRouter([
        tornado.web.Rule(routing.PathMatches("/picture.*"), picHandle),
    ])

    server = tornado.web.HTTPServer(router)
    return server

def initKafkaConsume():
    kafkaConsumer = Consumer({
        'bootstrap.servers': 'kfk013223.heracles.sohuno.com:9092,kfk013221.heracles.sohuno.com:9092,pdn096224.heracles.sohuno.com:9092,pdn096223.heracles.sohuno.com:9092,kfk013225.heracles.sohuno.com:9092,kfk013224.heracles.sohuno.com:9092,kfk013222.heracles.sohuno.com:9092,kfk013216.heracles.sohuno.com:9092,kfk013215.heracles.sohuno.com:9092,kfk131132.heracles.sohuno.com:9092,kfk131133.heracles.sohuno.com:9092,kfk131131.heracles.sohuno.com:9092,kfk013219.heracles.sohuno.com:9092,pdn096227.heracles.sohuno.com:9092,kfk013220.heracles.sohuno.com:9092,kfk131134.heracles.sohuno.com:9092,kfk013218.heracles.sohuno.com:9092,kfk013217.heracles.sohuno.com:9092,pdn096229.heracles.sohuno.com:9092,pdn096228.heracles.sohuno.com:9092,pdn096226.heracles.sohuno.com:9092,pdn096230.heracles.sohuno.com:9092,pdn096225.heracles.sohuno.com:9092,pdn096222.heracles.sohuno.com:9092,pdn096221.heracles.sohuno.com:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    })
    kafkaConsumer.subscribe(["mbads_dpa_goods_pic_test"])

if __name__ == "__main__":
    app = initServer()
    app.listen(REST_SERVER_PORT)
    print("server started")
    initKafkaConsume()
    print("kafka consume inited")
    tornado.ioloop.IOLoop.current().start()
