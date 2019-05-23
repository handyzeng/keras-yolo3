import os, sys
import argparse
import logging
from PIL import Image
import pickle
import zmq


FLAGS = None


def main():
    try:
        context = zmq.Context()

        recive = context.socket(zmq.PULL)
        recive.connect('tcp://{}:{}'.format(FLAGS.recv_addr, FLAGS.recv_port))

        sender = context.socket(zmq.PUSH)
        sender.connect('tcp://{}:{}'.format(FLAGS.send_addr, FLAGS.send_port))

        from yolo import YOLO
        detector = YOLO()

        while True:
            try:
                data = recive.recv()
                task = pickle.loads(data)
                if not isinstance(task, dict):
                    logging.warning("Wrong format task data received")
                    continue
                task_id = task['task_id']

                sender.send(data)
            except Exception as err:
                logging.error(str(err))

    except Exception as e:
        logging.error(str(e))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(argument_default=argparse.SUPPRESS)
    parser.add_argument(
        '--gpus', type=str, default="0", help='GPUs to use'
    ) 
    parser.add_argument(
        '--recv-addr', type=str, default="127.0.0.1", help='IP address of the task sender'
    ) 
    parser.add_argument(
        '--recv-port', type=str, default="5557", help='Socket port of the task sender'
    ) 
    parser.add_argument(
        '--send-addr', type=str, default="127.0.0.1", help='IP address of the result receiver'
    ) 
    parser.add_argument(
        '--send-port', type=str, default="5558", help='Socket port of the result receiver'
    ) 
    FLAGS = parser.parse_args()

    os.environ["CUDA_VISIBLE_DEVICES"] = FLAGS.gpu
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
    while True:
        main()


