import socket
import textacy
import spacy
import signal
import sys
import os
import json
import traceback


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


def RecognizeEntities(data, model, conn):
    doc = model(data)
    nouns = textacy.extract.noun_chunks(doc)
    for ent in nouns:
        d = {}
        d["start"] = ent.start_char
        d["end"] = ent.end_char
        d["type"] = ent.label_
        d["text"] = ent.text
        message = json.dumps(d) + "\n"
        conn.send(message.encode('ascii'))
    return True


def Main():
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
        en = spacy.load('en_core_web_md')
        print("Models loaded")
        while True:
            HandleConnections(sock, en)

    except KeyboardInterrupt:
        print("Closing")
        sock.close()
        os.remove(sockfile)
        sys.exit()

if __name__ == '__main__':
    Main()
