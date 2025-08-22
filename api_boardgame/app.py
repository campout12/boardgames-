from flask import Flask, jsonify, request
import subprocess


app = Flask(__name__)


@app.post("/submit")
def submit():
    try:
        data = request.json

        if not data or "app" not in data:
            raise ValueError("Invalid input: 'app' field is required.")
        app = data["app"]
        if type(app) is not str:
            raise ValueError("Invalid input: 'app' must be a string.")
        app = app.strip("./\\ \r\n\t")
        if not app:
            raise ValueError("Invalid input: 'app' cannot be empty.")

        master = None
        if "master" in data and type(data["master"]) is str:
            master = data["master"].strip("./\\ \r\n\t")
            if not master:
                raise ValueError("Invalid input: 'master' cannot be empty.")

    except ValueError as e:
        return jsonify({"error": e.args}), 400

    cmd = ["../bin/spark-submit"]
    if master:
        cmd += ["--master", master]
    cmd += [app]

    completed = subprocess.run(cmd, text=True, capture_output=True)

    if completed.returncode != 0:
        return jsonify({"error": completed.stderr or completed.stdout}), 500

    return jsonify({"output": completed.stdout}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
