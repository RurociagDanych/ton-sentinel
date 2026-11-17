"""Local alert sink: appends each Alertmanager webhook POST as one JSON line to /data/alerts.jsonl."""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

PATH = "/data/alerts.jsonl"


class H(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        try:
            line = json.dumps(json.loads(body), separators=(",", ":"))
        except ValueError:
            self.send_response(400)
            self.end_headers()
            return
        with open(PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass


HTTPServer(("0.0.0.0", 9095), H).serve_forever()
