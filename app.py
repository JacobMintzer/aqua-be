import json
from flask import Flask, request
from database import Client

with open("./database.json", "r") as data:
    credentials = json.load(data)

client = Client(credentials)

app = Flask(__name__)


@app.route("/search", methods=["GET"])
def search():
    try:
        term = request.args.get("query", "")
        page = request.args.get("page", 0, type=int)
        exact = request.args.get("exact", "False")
        (count, result) = client.search(term, int(page), exact.title() == "True")
        response = {"count": count, "results": result}
        return json.dumps(response), 200
    except Exception as e:
        return e, 500


if __name__ == "__main__":
    app.run()
