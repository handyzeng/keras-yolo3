import zmq
import threading
import pickle
import json

def thread_send():
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.setsockopt(zmq.SNDHWM, 100)
    socket.bind('tcp://*:5557')

    id = 0
    while True:
        task_id = "{}".format(id)
        with open('./data/dog.jpg','rb') as f:
            image = f.read()
        task = dict(task_id=task_id, images=[{'data':image}])
        socket.send(pickle.dumps(task))
        print("SEND: " + task['task_id'])
        id = id + 1

def thread_recv():
    context = zmq.Context()

    socket = context.socket(zmq.PULL)
    socket.setsockopt(zmq.RCVHWM, 100)
    socket.bind('tcp://*:5558')

    while True:
        data = socket.recv()
        task = pickle.loads(data)
        print("RECV: " + task['task_id'])


def main():
    t1 = threading.Thread(target=thread_send, args=())
    t2 = threading.Thread(target=thread_recv, args=())
    t1.start()
    t2.start()

    t1.join()
    t2.join()


main()


