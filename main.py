import traceback
from gevent import monkey
from gevent.pywsgi import WSGIServer
from flask_compress import Compress
from flask import Flask, request, abort, render_template
from api.process import RPC
monkey.patch_all()


app = Flask('HousingAnalytics')

compress = Compress()
compress.init_app(app)


@app.route('/')
def main_page():
    return render_template('main.html')


@app.route('/process', methods=['POST'])
def process():
    function = request.form.get('function')
    try:
        return RPC.get(function)()
    except Exception as e:
        print(traceback.format_exc())
        abort(500, f'{str(e)}\nRefresh page process same file(s) again! 🥠')


HOST = '0.0.0.0'
PORT = 8000
http_server = WSGIServer((HOST, PORT), app)
print(f'Listening at http://{HOST}:{PORT} 🚀')
http_server.serve_forever()
