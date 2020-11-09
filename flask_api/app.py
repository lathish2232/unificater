from flask import Flask, Response, request
from flask_restplus import reqparse, Api, Resource, fields
from flask_cors import CORS
import json,os

#user modules
from utility.logger import init_logger, log, metrics_logging
from middleware.filesource import FilesourceMiddleware
from middleware.Databasesource import DatabasesourceMiddleware
from middleware.unificater_flow import UniflowMiddleware
from middleware.instance import InstancesMiddleware
from etl.database.uni_metadata import uni_metadata


app = Flask(__name__, static_url_path='/static/')
app.config['CORS_HEADERS'] = 'Content-Type'
Cors = CORS(app)

api = Api(app)
init_logger()

# Defining api models from documentation
model_400 = api.model('Errorresponse400', {'message': fields.String, 'errors': fields.Raw})
model_500 = api.model('Errorresponse400', {'message': fields.Integer, 'errors': fields.String})
model_health_200 = api.model('successResponse200', {'success': fields.Boolean, 'status': fields.Integer})

log.info("AB-server api started Successfully")

# middle ware object initialized
fp = DatabasesourceMiddleware()
@api.route('/unificaterflow', methods=['GET'])
@api.response(200, 'Successful')
@api.response(400, 'validation Error', model_400)
@api.response(500, 'Internal processing Error', model_500)
class unificaterflow(Resource):
    def get(self):
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = UniflowMiddleware()
            return_status, result = fp.run(request)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = Response(json.dumps(result),status=return_status, mimetype="application/json")
        return resp

    def post(self,id=None):
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = UniflowMiddleware()
            return_status, result = fp.run(request,id)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = Response(json.dumps(result),status=return_status, mimetype="application/json")
        return resp
api.add_resource(unificaterflow, '/unificaterflow/update/<id>', methods=['POST'])

@api.route('/unificaterflow/allflows')
@api.response(200, 'Successful')
@api.response(400, 'validation Error', model_400)
@api.response(500, 'Internal processing Error', model_500)
class allflows(Resource):
    def get(self,id =None):
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = UniflowMiddleware()
            return_status, result = fp.active(request,id)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = Response(json.dumps(result),status=return_status, mimetype="application/json")
        return resp
api.add_resource(allflows, '/unificaterflow/allflows/<id>', methods=['GET'])

@api.route('/connections', methods=['GET'])
@api.response(200, 'Successful')
@api.response(400, 'validation Error', model_400)
@api.response(500, 'Internal processing Error', model_500)
class connections(Resource):
    def get(self,id=None,connection_id=None,isfwf=None):
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = UniflowMiddleware()
            return_status, result = fp.run_connection(request,id,connection_id,isfwf)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = Response(json.dumps(result),status=return_status, mimetype="application/json")
        return resp

    def post(self):
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = UniflowMiddleware()
            return_status, result = fp.run_connection(request,True)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = Response(json.dumps(result),status=return_status, mimetype="application/json")
        return resp
api.add_resource(connections,'/connections/<id>', methods=['GET'])
api.add_resource(connections,'/connections/<id>/<connection_id>', methods=['GET'])
api.add_resource(connections,'/connections/<id>/<connection_id>/<isfwf>', methods=['GET'])

@api.route('/instances')
@api.response(200, 'Successful')
@api.response(400, 'validation Error', model_400)
@api.response(500, 'Internal processing Error', model_500)
class Instances(Resource):
    def get(self):
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = InstancesMiddleware()
            return_status, result = fp.run(request, False)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = Response(json.dumps(result),status=return_status, mimetype="application/json")
        return resp

    def post(self):
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = InstancesMiddleware()
            return_status, result = fp.run(request, True)
        except Exception as e:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['message'] = 'Internal Error has Occurred while processing the File request'
            result['status'] = 0
        finally:
            resp = Response(json.dumps(result),
                            status=return_status, mimetype="application/json")
        return resp
api.add_resource(Instances, '/instances/all', methods=['GET'])
api.add_resource(Instances, '/instances', methods=['GET'])
api.add_resource(Instances, '/instances', methods=['POST'])

@api.route('/instances/data_instance')
@api.response(200, 'Successful')
@api.response(400, 'validation Error', model_400)
@api.response(500, 'Internal processing Error', model_500)
class data_instance(Resource):
    def get(self):
        return_status = None
        result = {}
        try:
            log.info("instance Request Initiated")
            fp = InstancesMiddleware()
            return_status, result = fp.data_instance(request)
        except:
            result = {}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] = 0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = Response(json.dumps(result),status=return_status, mimetype="application/json")
        return resp
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 7197))
    log.info(port)
    log.info("runing ...")
    app.run(host='0.0.0.0', port=port)
