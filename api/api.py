import tornado.web
import json
from models.models import AggregationRequest
from core.ctr import CTR_Calculater

class BusinessAnalysisHandler(tornado.web.RequestHandler):

    def post(self, model=None):
        '''
        This function takes a request and calculates
        the click through rate base on that data, as well
        as validates the data structure of request and response.
        It returns a list of objects in following formats:

        Request parameters:

        startTimestamp : mandatory parameter (in the format of '2016-01-03 13:55:00'
        endTimestamp : mandatory parameter in the format of '2016-01-04 13:55:00'
        aggregation : mandatory parameter, the interval aggregation in minutes
        product : optional parameter as a string
        platform : optional parameter as a string

        Response parameters:

        timestamp : initial timestamp of each aggregation
        platform : platform as explained above
        product : product as explained above
        CTR : metric calculated as the #purchases / #productViews
        '''

        data = json.loads(self.request.body.decode('utf-8'))

        try:
            request_model = AggregationRequest(data)
            request_model.validate()
        except:
            raise "400"

        try:
            ctr_calculator = CTR_Calculater(data)
            response = {"ctr_response": ctr_calculator.calculate_ctr()}
            response = json.dumps(response)
            self.write(response)
        except:
            raise "500"
