import tornado.ioloop
import tornado.web
import tornado.routing as routing


REST_SERVER_PORT = 8888
kafkaConsumer = None

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write({"data":"ok"})

def initServer():
    picHandle = tornado.web.Application([
        (r"/", MainHandler),
    ])

    router = routing.RuleRouter([
        tornado.web.Rule(routing.PathMatches("/"), picHandle),
    ])

    server = tornado.web.HTTPServer(router)
    return server

if __name__ == "__main__":
    app = initServer()
    app.listen(REST_SERVER_PORT)
    print("server started")
    tornado.ioloop.IOLoop.current().start()
