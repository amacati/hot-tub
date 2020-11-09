# Webserver functionality and dynamic web pages
from flask import Flask, render_template

from pathlib import Path


PATH = Path().joinpath('static', 'global_temp_diff_map.png')

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0  # No caching.


# How requests for the route (127.0.0.1:5000) should be handled
@app.route('/')
def index():
    return render_template('index.html', temperature_map=PATH)


# How requests for the route (127.0.0.1:5000/about) should be handled
@app.route('/about')
def about():
    return render_template('about.html')


@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0')
