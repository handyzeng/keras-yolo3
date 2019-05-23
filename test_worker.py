import zmq
import threading
import pickle

def thread_send():
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)

    socket.bind('tcp://*:5557')

    id = 0
    while True:
        task_id = "{}".format(id)
        data = dict(task_id=task_id)
        socket.send(pickle.dumps(data))
        print("task: {} sended".format(task_id))
        id = id + 1

def thread_recv():
    context = zmq.Context()

    socket = context.socket(zmq.PULL)
    socket.bind('tcp://*:5558')

    while True:
        data = socket.recv()
        task = pickle.loads(data)
        print("task: {} received".format(task['task_id']))


def main():
    t1 = threading.Thread(target=thread_send, args=())
    t2 = threading.Thread(target=thread_recv, args=())
    t1.start()
    t2.start()

    t1.join()
    t2.join()


main()


