import zmq
import time
import msgpack as mp
import array as arr


"""
PUB = 1
SUB = 2
REQ = 3
REP = 4

PULL = 7
PUSH = 8


ep = end_point
eps = ep_list
pkt = packet
msg = message
ctx = zmq.Context()
ss = socket_list
t = topic

init_msg:
  {'eps': [{ 'socket_type': X, 'endpoint': Y, sub_topics:['blah', 'blip] }]}

task_msg:
    this is a blob that 'task' function knows how to handle

"""

EP_TYPES = [zmq.PUB, zmq.SUB, zmq.PUSH, zmq.PULL, zmq.REQ, zmq.REP,
            zmq.XPUB, zmq.XPUB, zmq.XSUB, zmq.STREAM, zmq.DEALER, zmq.ROUTER]

ctx = zmq.Context()


def config(init_msg):

    eps = init_msg['eps']
    ss = []
    for ep in eps:
        sout = ep['stdout_socket']
        s = ctx.socket(ep['socket_type'])
        s.connect(ep['end_point'])
        # setsockopts
        if ep['socket_type'] == zmq.SUB:
            ts = ""
            for t in ep['sub_topics']:
                ts += t + ','
            ts = ts[:-2]
            s.setsockopt(zmq.SUBSCRIBE, ts)
        ss.append(s)
    return ss


def kill():
    ctx.destroy()


def task(msg):
    pass


def encode(msg):
    return mp.packb(msg)


def decode(pkt):
    return mp.unpackb(pkt)


def run():

    # Initialize poll set
    plr = zmq.Poller()

    # Initialize configuration socket. The first msg is alway the configuration message.
    s = ctx.socket(zmq.SUB)
    plr.register(s, zmq.POLLIN)

    # Process messages from both sockets

    while True:
        try:
            plr_ss = dict(plr.poll())
        except:
            KeyboardInterrupt:
                break
        for ps in plr_ss:
            msg = s.recv()
            id  = msg['id']

    # # Initialize poll set
    # plr = zmq.Poller()

    # # Initialize configuration socket. The first msg is alway the configuration message.
    # s = ctx.socket(zmq.SUB)
    # plr.register(s, zmq.POLLIN)

    # # Process messages from both sockets
    # flag = True
    # ss = []
    # while True:
    #     try:
    #         socks = dict(plr.poll())
    #     except KeyboardInterrupt:
    #         break

    #     if flag:
    #         flag = False
    #         ss = config(s.recv())

    #     for s in ss:

    #         if s in socks:
    #             task(s.recv())
    # kill()
