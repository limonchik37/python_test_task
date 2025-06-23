#!/usr/bin/python3
import threading
import queue
import time
from random import randint
from datetime import datetime
import argparse  # for debug flag

NCLIENTS = 10  # Number of emulated clients and corresponding client queues

def now():
    return datetime.now().strftime("%H:%M:%S")

def server_enqueue(qin, msg):
    '''Send message msg to server using queue qin'''
    tag = randint(0, 100000000)
    msg["tag"] = tag
    qin.put(msg)
    return tag

def server_dequeue(qout):
    '''Get message and it's tag from qout queue'''
    if not qout.empty():
        msg = qout.get()
        if "tag" in msg:
            tag = msg["tag"]
            msg.pop("tag")
            return (msg, tag)
    return ({}, -1)

def server_worker(qin, qout):
    '''Emulates server processing requests received in qin Queue.
    Results are sent to qout Queue.'''
    pbuf = []
    while True:
        if not qin.empty():
            request = qin.get()
            print("Server received message", request)
            request["send_time"] = time.time() + randint(1, 20) / 10
            pbuf.append(request)
        tnow = time.time()
        for msg in pbuf:
            if msg["send_time"] <= tnow:
                msg.pop("send_time")
                print("Server sending message:", msg)
                qout.put(msg)
        pbuf[:] = [x for x in pbuf if "send_time" in x]

def client_worker(arrqin, arrqout):
    '''Emulates clients generating requests'''
    while True:
        nc = randint(0, NCLIENTS - 1)
        msg = {
            "body": f"Testing message from client {nc}, id:#{randint(0, 1024)}"
        }
        print(f"Client {nc} sends message", msg)
        arrqout[nc].put(msg)

        for j in range(NCLIENTS):
            while not arrqin[j].empty():
                print(f"Client {j} received message:", arrqin[j].get())
        time.sleep(1)

class Middleware:
    def __init__(self, s_qin, s_qout, ac_qin, ac_qout, debug=False):
        """
        Initialize the middleware with queues for server and clients.
        :param s_qin: Queue for sending requests to the server
        :param s_qout: Queue for receiving responses from the server
        :param ac_qin: List of input queues for each client (responses)
        :param ac_qout: List of output queues for each client (requests)
        :param debug: Enable debug logging
        """
        self.s_qin = s_qin
        self.s_qout = s_qout
        self.ac_qin = ac_qin
        self.ac_qout = ac_qout
        self.tag_to_client = {}  # Maps tags to client indices
        self.debug = debug

    def log(self, msg, level="INFO"):
        """
        Print middleware logs depending on debug flag or severity level.
        :param msg: Log message
        :param level: Severity level (INFO, WARNING, ERROR)
        """
        if self.debug or level in ("ERROR", "WARNING"):
            print(f"[{now()}] [MIDDLEWARE] [{level}] {msg}")

    def handle_client_requests(self):
        """
        Check each client output queue for new messages.
        If a message exists, forward it to the server and store the tag-client mapping.
        """
        for i, client in enumerate(self.ac_qout):
            try:
                if not client.empty():
                    message = client.get()
                    tag = server_enqueue(self.s_qin, message)
                    self.tag_to_client[tag] = i
                    self.log(f"Got message from client {i}, assigned tag {tag}")
            except Exception as e:
                self.log(f"While handling client {i}: {e}", level="ERROR")

    def handle_server_responses(self):
        """
        Check server response queue. If a response is available, match its tag and
        route it to the appropriate client input queue.
        """
        if not self.s_qout.empty():
            response, tag = server_dequeue(self.s_qout)
            if tag in self.tag_to_client:
                client_idx = self.tag_to_client.pop(tag)
                self.ac_qin[client_idx].put(response)
                self.log(f"Sending response with tag {tag} to client {client_idx}")
            else:
                self.log(f"Received unknown tag from server: {tag}", level="WARNING")

    def step(self):
        """
        Continuously handle messages between clients and the server.
        This method runs the middleware loop indefinitely.
        """
        while True:
            self.handle_client_requests()
            self.handle_server_responses()

def parse_args():
    parser = argparse.ArgumentParser(description="Middleware Application")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()

    server_qout = queue.Queue()
    server_qin = queue.Queue()
    ServerThread = threading.Thread(target=server_worker,
                                    args=(server_qin, server_qout), daemon=True)
    ServerThread.start()

    arrclient_qin = []
    arrclient_qout = []
    for i in range(NCLIENTS):
        arrclient_qin.append(queue.Queue())
        arrclient_qout.append(queue.Queue())

    ClientThread = threading.Thread(target=client_worker,
                                    args=(arrclient_qin, arrclient_qout), daemon=True)
    ClientThread.start()

    Middleware(server_qin, server_qout, arrclient_qin, arrclient_qout, debug=args.debug).step()