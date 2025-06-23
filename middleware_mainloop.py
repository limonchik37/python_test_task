#!/usr/bin/python3
import threading
import queue
import time
from random import randint
from datetime import datetime # time logs


NCLIENTS = 10    # Number of emulated clients and corresponding client queues

def now():
    return datetime.now().strftime("%H:%M:%S")

def handle_client_requests(ac_qout, s_qin, tag_to_client):
    # Iterate over all client output queues
    for i, client in enumerate(ac_qout):
        try:
            # If client has a message, retrieve it
            if not client.empty():
                message = client.get()
                # Send message to server and get assigned tag
                tag = server_enqueue(s_qin, message)
                # Store tag-to-client index mapping
                tag_to_client[tag] = i
                print(f"[{now()}] [MIDDLEWARE] Got message from client {i}, assigned tag {tag}")
        except Exception as e:
            # Log any exceptions for client handling
            print(f"[{now()}] [ERROR] While handling client {i}: {e}")

def handle_server_responses(s_qout, ac_qin, tag_to_client):
    # If server has responded with a message
    if not s_qout.empty():
        server_response, tag = server_dequeue(s_qout)
        # Check if the tag is known
        if tag in tag_to_client:
            client_idx = tag_to_client.pop(tag)
            # Route the response to the correct client input queue
            ac_qin[client_idx].put(server_response)
            print(f"[{now()}] [MIDDLEWARE] Sending response with tag {tag} to client {client_idx}")
        else:
            # Handle unknown tag
            print(f"[{now()}] [WARNING] Received unknown tag from server: {tag}")

def main_loop(s_qin, s_qout, ac_qin, ac_qout):
    '''
 To send message msg to emulated server use:
       server_enqueue(ServerQin, <msg>).
       Returns message unique tag
 To get message and it's unique tag use:
       server_dequeue(ServerQout).
       Returns tuple (<msg>, <tag>)
 To find out if message queue is empty use:
       .empty()    , e.g. ServerQout.empty() or ArrClientQin[0].empty()
    '''

# -------- HERE GOES YOUR CODE --------
    # Dictionary to map server-assigned tag to client index
    tag_to_client = {}

    while True:

        # Step 1: Check for messages from any client
        handle_client_requests(ac_qout, s_qin, tag_to_client)
        
        # Step 2: Check for responses from server and route them back to clients
        handle_server_responses(s_qout, ac_qin, tag_to_client)
# -------------------------------------

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
        if "tag" in msg.keys():
            tag = msg["tag"]
            msg.pop("tag")
            return (msg, tag)
    return ({}, -1)


def server_worker(qin, qout):
    '''Emulates server processing requests received in qin Queue.
    Results are sent to qout Queue.'''

    pbuf = []

    while True:
        # Process input queue
        if not qin.empty():
            request = qin.get()
            print("Server received message ", request)
            # 0.1..2s emulated processing delay
            request["send_time"] = time.time() + randint(1, 20)/10
            pbuf.append(request)
        # Process message buffer
        tnow = time.time()
        for msg in pbuf:
            if msg["send_time"] <= tnow:
                msg.pop("send_time")
                print("Server sending message: ", msg)
                qout.put(msg)
        pbuf[:] = [x for x in pbuf if "send_time" in x.keys()]


def client_worker(arrqin, arrqout):
    '''Emulates clients generating requests'''

    while True:
        nc = randint(0, NCLIENTS-1)
        msg = {"body" : "Testing message from client " + \
                str(nc) + ", id:#" + str(randint(0, 1024))}
        print("Client " + str(nc) + " sends message ", msg)
        arrqout[nc].put(msg)
        for j in range(NCLIENTS):
            while not arrqin[j].empty():
                print("Client " + str(j) + " received message: ", arrqin[j].get())
        time.sleep(1)


if __name__ == '__main__':
    server_qout = queue.Queue()
    server_qin = queue.Queue()
    ServerThread = threading.Thread(target=server_worker, \
        args=(server_qin, server_qout), daemon=True)
    ServerThread.start()

    arrclient_qin = []
    arrclient_qout = []
    for i in range(NCLIENTS):
        arrclient_qin.append(queue.Queue())
        arrclient_qout.append(queue.Queue())
    ClientThread = threading.Thread(target=client_worker, \
            args=(arrclient_qin, arrclient_qout), daemon=True)
    ClientThread.start()

    main_loop(server_qin, server_qout, arrclient_qin, arrclient_qout)