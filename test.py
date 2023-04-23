import requests
from flask import Flask
from flask import jsonify


instance_metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/"
metadata_entries_to_grab = {"id": "instance-id", "name": "instance-name"}
metadata_flavor_header = {"Metadata-Flavor": "Google"}

app = Flask(__name__)


@app.route('/')
def main():
    return_resp = {"message": "hello world!"}
    return jsonify(return_resp)
