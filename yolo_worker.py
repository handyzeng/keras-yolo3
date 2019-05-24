import os, sys
import argparse
import logging
from PIL import Image
import tensorflow as tf
import numpy as np
import threading
import pickle
import queue
import urllib
import time
import wget
import zmq
import cv2

def thread_load(zmqCtx, qLoad):
    while True:
        try:
            recive = zmqCtx.socket(zmq.PULL)
            recive.connect('tcp://{}:{}'.format(FLAGS.recv_addr, FLAGS.recv_port))
            while True:
                try:
                    task = pickle.loads(recive.recv())
                    task_id = task['task_id']
                    for i, img in enumerate(task['images']):
                        if 'data' in img:
                            data = img['data']
                        elif 'path' in img:
                            data = open(img['path'], 'rb').read()
                        elif 'url' in img:
                            data = urllib.urlopen(img['url']).read()
                        else:
                            logging.warning('THREAD-LOAD: ' + '{} ... wrong image data'.format(task_id))
                            continue
                        imgdata = np.asarray(bytearray(data), dtype="uint8")
                        cvimg = cv2.imdecode(imgdata, cv2.IMREAD_COLOR)
                        image = Image.fromarray(cv2.cvtColor(cvimg, cv2.COLOR_BGR2RGB))
                        img['image'] = image
                        task['images'][i] = img
                    qLoad.put(task)
                    logging.info('THREAD-LOAD: {}'.format(task_id))
                except Exception as e:
                    logging.error('THREAD-LOAD: ' + str(e))
        except Exception as e:
            logging.error('THREAD-LOAD: ' + str(e))
            time.sleep(1)

def thread_save(zmqCtx, qSave):
    while True:
        try:
            sender = zmqCtx.socket(zmq.PUSH)
            sender.connect('tcp://{}:{}'.format(FLAGS.send_addr, FLAGS.send_port))
            while True:
                try:
                    task = qSave.get()
                    sender.send(pickle.dumps(task))
                except Exception as err:
                    logging.error('THREAD-SAVE: ' + str(err))
        except Exception as e:
            logging.error('THREAD-SAVE: ' + str(e))

def thread_work(qLoad, qSave):
        config = tf.ConfigProto()
        config.gpu_options.per_process_gpu_memory_fraction = 0.20
        with tf.Session(config=config) as sess:
            from yolo import YOLO
            detector = YOLO()
            while True:
                try:
                    task = qLoad.get()
                    task_id = task['task_id']
                    for i, img in enumerate(task['images']):
                        img['objs'] = detector.detect(img['image'])
                        del(img['image'])
                        task['images'][i] = img
                    logging.info('THREAD-WORK: {}'.format(task_id))
                    qSave.put(task)
                except Exception as err:
                    logging.error('THREAD-WORK: ' + str(err))

def main():
    try:
        context = zmq.Context()
        queueLoad = queue.Queue(FLAGS.queue_size)
        queueSave = queue.Queue(FLAGS.queue_size)
        t1 = threading.Thread(target=thread_work, args=(queueLoad, queueSave, ))
        t2 = threading.Thread(target=thread_save, args=(context, queueSave, ))
        t3 = threading.Thread(target=thread_load, args=(context, queueLoad, ))
        t1.start()
        t2.start()
        t3.start()

        t1.join()
        t2.join()
        t3.join()
    except Exception as e:
        logging.error(str(e))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(argument_default=argparse.SUPPRESS)
    parser.add_argument(
        '--gpu', type=str, default="0", help='GPUs to use'
    ) 
    parser.add_argument(
        '--recv-addr', type=str, default="127.0.0.1", help='IP address of the task sender'
    ) 
    parser.add_argument(
        '--recv-port', type=int, default=5557, help='Socket port of the task sender'
    ) 
    parser.add_argument(
        '--send-addr', type=str, default="127.0.0.1", help='IP address of the result receiver'
    ) 
    parser.add_argument(
        '--send-port', type=int, default=5558, help='Socket port of the result receiver'
    )
    parser.add_argument(
        '--queue-size', type=int, default=100, help='Socket port of the result receiver'
    )
    parser.add_argument(
        '--cache-path', type=str, default="./cache", help='IP address of the result receiver'
    ) 
    FLAGS = parser.parse_args()

    os.environ["CUDA_VISIBLE_DEVICES"] = FLAGS.gpu
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')

    if not os.path.exists(FLAGS.cache_path):
        os.makedirs(FLAGS.cache_path)

    main()


