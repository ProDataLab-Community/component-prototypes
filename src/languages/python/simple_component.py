import zmq
import msgpack as mp
import task

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
"""

EP_TYPES = [zmq.PUB, zmq.SUB, zmq.PUSH, zmq.PULL, zmq.REQ, zmq.REP,
            zmq.XPUB, zmq.XPUB, zmq.XSUB, zmq.STREAM, zmq.DEALER, zmq.ROUTER]
ctx = zmq.Context()
plr = zmq.Poller()

def config(config_msg):

    inputs  = config_msg['zmq_sockets']['inputs']
    outputs = config_msg['zmq_sockets']['outputs']
    in_sockets     = []
    out_sockets    = []

    for i in inputs:
        st = i['zmq_socket_type']
        ep = i['endpoint']
        s = ctx.socket(st)
        s.connect(ep)
        s_type = s.get(zmq.ZMQ_TYPE)
        if s_type == zmq.SUB:
            s.setsockopt(zmq.SUBSCRIBE, b'10001')
        plr.register(s, zmq.POLLIN)
        in_sockets.append(s)
    for o in outputs:
        st = o['zmq_socket_type']
        ep = o['endpoint']
        s = ctx.socket(st)
        s.bind(ep)
        s_type = s.get(zmq.ZMQ_TYPE)
        plr.register(s, zmq.POLLOUT)
        out_sockets.append

def kill():
    ctx.destroy()

def encode(msg):
    return mp.packb(msg)

def decode(pkt):
    return mp.unpackb(pkt)

def run(config_endpoint):
                                # Initialize poll set
    plr = zmq.Poller()
                                # Initialize configuration socket. The first msg is alway the configuration message.
    s = ctx.socket(zmq.SUB)
    s.connect(config_endpoint)
    s.setsockopt(zmq.SUBSCRIBE, b'10001')
    plr.register(s, zmq.POLLIN)
                                # Process messages from sockets
    while True:
        try:
            plr_ss = dict(plr.poll())
        except KeyboardInterrupt:
                break
        for ps in plr_ss:                                       # FIXME .. handle POLLERR, etc.       zmq.POLLERR, zmq.POLLOUT
            if plr_ss[ps] != zmq.POLLIN:
                continue                        
            msg = decode(ps.recv())
            id  = msg['id']
            task_data = msg['task_data']
                                            # NOTE: this is temporary simplification
            if id == 'configure':        
                config(task_data)
                continue
            if id == 'kill':
                kill()
            msg = task(task_data)
            ps.send(encode(msg))
    kill()
