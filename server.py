import socket
# import textacy
import spacy
import signal
import sys
import os
import json
import traceback
import mmh3
import en_core_web_sm
from flask import Flask, Response, stream_with_context, request
import time

wall = "http://wallscope.co.uk/ontology/nlp/"
rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
rdfs = "http://www.w3.org/2000/01/rdf-schema#"
wall_text = "http://wallscope.co.uk/resource/annotation/"


def HandleConnections(s, model):
    conn, addr = s.accept()
    while True:
        try:
            data = conn.recv(10240).decode()
            if not data:
                break
            RecognizeEntities(data, model, conn)
            break
        except Exception:
            print("EXCEPTION!!!!")
            traceback.print_exc()
            break
    # Close the connection
    conn.close()


def jsonify(ent):
    d = {}
    d["start"] = ent.start_char
    d["end"] = ent.end_char
    d["type"] = ent.label_
    d["text"] = ent.text
    return json.dumps(d) + "\n"


def turtleify(ent):
    start = str(ent.start_char)
    end = str(ent.end_char)
    unique = str(mmh3.hash(ent.text))
    uri = "<" + wall_text + "uid-" + unique + "> "
    message = uri
    message += "<"+wall+"start> "+start+" ; "
    message += "<"+wall+"end> "+end+" ; "
    message += "<"+wall+"annotationType> <"+wall_text+"type/"+ent.label_+"> ; "
    message += "<"+wall+"annotationText> "+json.dumps(ent.text)+" . \n"
    return message


def RecognizeEntities(data, model, conn):
    doc = model(data)
    # nouns = textacy.extract.noun_chunks(doc)
    for ent in doc.ents:
        message = turtleify(ent)
        conn.send(message.encode('ascii'))
    return True

def RecognizeEntitiesGenerator(data, model):
    doc = model(data)
    for ent in doc.ents:
        message = turtleify(ent)
        yield message
    return True

def Socket():
    # Socket boilerplate
    sockfile = "/tmp/nlp-socket.sock"

    if os.path.exists(sockfile):
        os.remove(sockfile)

    # Interrupt handling boilerplate
    signal.signal(signal.SIGINT, signal.default_int_handler)

    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(sockfile)
        # Create a pool of 20 connections
        sock.listen(20)
        print("Socket listening")
        # Import the English NLP Models
        print("Loading Models")
        en = spacy.load('en_core_web_sm')
        # en = en_core_web_sm.load()
        print("Models loaded")
        while True:
            HandleConnections(sock, en)

    except KeyboardInterrupt:
        print("Closing")
        sock.close()
        os.remove(sockfile)
        sys.exit()

app = Flask(__name__)

print("Socket listening")
# Import the English NLP Models
print("Loading Models")
en = spacy.load('en_core_web_sm')
# en = en_core_web_sm.load()
print("Models loaded")

@app.route('/', methods=['POST'])
def index():
    text = request.form['text']
    return Response(stream_with_context(RecognizeEntitiesGenerator(text, en)))

def WebServer():
    app.run(debug=True, host='0.0.0.0', port=5005)

if __name__ == '__main__':
    WebServer()
