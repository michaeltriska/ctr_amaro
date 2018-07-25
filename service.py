from tornado.ioloop import IOLoop
import tornado.web
from api.api import BusinessAnalysisHandler

"""
Overview

The applications use Tornado as a HTTP server,
and Schematics for dealing with representations.

Project structure
The typical application is structured in submodules:

    app
        api - api handler
        core - domain implementation, i.e. crud operatios on representations
        service.py - the service class
        helper - the configuration
"""


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [(r"/crt/v2", BusinessAnalysisHandler)]
        tornado.web.Application.__init__(self, handlers)

def main():
    app = Application()
    app.listen(80)
    IOLoop.instance().start()


if __name__ == '__main__':
    main()
