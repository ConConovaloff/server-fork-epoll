# -*- coding: utf-8 -*-

#
# Not for production (unstable). Only for study.
#
# writen on the basis of https://github.com/crustymonkey/py-prefork-server
#    Author: Jay Deiman
#    Email: admin@splitstreams.com
#
#    Author: ConConovaloff
#    Email: constantin  conovaloff.com
#

import multiprocessing as mp
import select
import threading
import socket
import os
import time
import sys


class pfe:
    WAITING = 1
    BUSY = 2
    EXITING_ERROR = 4
    EXITING_MAX = 8
    EXITING = EXITING_ERROR | EXITING_MAX
    CLOSE = 16

    EVENT_NAMES = {
        WAITING: 'WAITING',
        BUSY: 'BUSY',
        EXITING_ERROR: 'EXITING_ERROR',
        EXITING_MAX: 'EXITING_MAX',
        EXITING: 'EXITING',
        CLOSE: 'CLOSE',
    }


class ManagerChild(object):
    """
    Class to represent a child in the Manager
    """

    def __init__(self, pid, parent_conn):
        self.pid = pid
        self.conn = parent_conn
        # self.current_state = pfe.WAITING
        self.total_processed = 0

    def close(self):
        self.conn.close()


class Server:

    def __init__(self):
        self.bind_ip = '127.0.0.1'
        self.port = 10000
        self.listen = 5
        self.workers = 5

        self._children = {}

        self._poll = select.epoll(select.EPOLLIN)

        self._stop = threading.Event()

        self._bind()

    def log(self, msg):
        print 'Server ' + str(os.getpid()) + ': ' + str(msg)

    def _bind(self):
        """
        Bind the socket
        """
        self.log('work on ip:' + str(self.bind_ip) + ' port:' + str(self.port))
        address = (self.bind_ip, self.port)
        protocol = socket.SOCK_STREAM

        self.server_socket = socket.socket(socket.AF_INET, protocol)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        self.server_socket.bind(address)
        self.server_socket.listen(self.listen)

    def run(self):
        self._init_children()
        self._loop()
        self._shutdown_server()

    def _init_children(self):
        for i in range(self.workers):
            self._start_child()

    def _start_child(self):
        """
        Fork off a child and set up communication pipes
        """
        parent_pipe, child_pipe = mp.Pipe()
        self._poll.register(parent_pipe.fileno(), select.POLLIN | select.POLLPRI)

        pid = os.fork()
        if not pid:
            ch = Worker(child_pipe, self.server_socket)
            parent_pipe.close()
            ch.run()
        else:
            self._children[parent_pipe.fileno()] = ManagerChild(pid, parent_pipe)
            child_pipe.close()

    def _loop(self):
        while True:

            if self._stop.isSet():
                break

            for file_no, e in self._poll.poll(1, 10):
                self.log('poll: ' + repr([file_no, e]))

                if file_no in self._children:
                    ch = self._children[file_no]
                    self._handle_child_event(ch)
                else:
                    raise Exception('not find fd:' + str(file_no))

    def _handle_child_event(self, child):
        event, msg = child.conn.recv()
        event = int(event)
        if event & pfe.EXITING:
            fd = child.conn.fileno()
            self._poll.unregister(child.conn)
            del self._children[fd]
            child.close()
            os.waitpid(child.pid, 0)
        else:
            child.current_state = int(event)
            child.total_processed = int(msg)

    def _shutdown_server(self):
        children = self._children.values()
        for child in children:
            self._kill_child(child, False)
        if self.server_socket:
            self.server_socket.close()

    def _kill_child(self, child, background=True):
        fd = child.conn.fileno()

        self._poll.unregister(child.conn)
        child.conn.send([pfe.CLOSE, ''])
        child.close()

        if fd in self._children:
            del self._children[fd]

        if background:
            t = threading.Thread(target=os.waitpid, args=(child.pid, 0))
            t.daemon = True
            t.start()
        else:
            os.waitpid(child.pid, 0)


class Worker:

    def __init__(self, child_conn, server_socket):
        self._server_socket = server_socket
        self._child_conn = child_conn
        self._poll = select.epoll()
        self._poll.register(self._server_socket, select.POLLIN | select.POLLPRI)
        self._poll.register(self._child_conn, select.POLLIN | select.POLLPRI)
        self.closed = False
        self.requests_handled = 0

    @classmethod
    def log(cls, msg):
        print 'Worker ' + str(os.getpid()) + ': ' + str(msg)

    def run(self):
        self.log('start: server_fd:' + str(self._server_socket.fileno()) +
                 ' child_fd:' + str(self._child_conn.fileno()))

        self._loop()

    def _loop(self):
        while True:

            for file_no, e in self._poll.poll():
                self.log('poll: ' + repr([file_no, e]))

                if file_no == self._server_socket.fileno():
                    try:
                        self._handle_connection()
                    except Exception, e:
                        self._error(e)
                        self._shutdown(1)
                    self.requests_handled += 1

                elif file_no == self._child_conn.fileno():
                    self._handle_parent_event()

            if self.closed:
                self._shutdown()

    def _handle_parent_event(self):
        try:
            event, msg = self._child_conn.recv()
        except EOFError:
            self.closed = True
            return

        event = int(event)
        if event & pfe.CLOSE:
            self.closed = True

    def _shutdown(self, status=0):
        self._poll.unregister(self._child_conn)
        self._poll.unregister(self._server_socket)
        self._child_conn.close()
        self._server_socket.close()
        time.sleep(0.1)
        sys.exit(status)

    def _handle_connection(self):
        self.log('before accept')
        self.conn, self.address = self._server_socket.accept()
        self.log('after accept')

        self._busy()
        self.process_request()
        self._close_conn()
        self._waiting()

    def _close_conn(self):
        if self.conn and isinstance(self.conn, socket.SocketType):
            self.conn.close()

    def _waiting(self):
        self._child_conn.send([pfe.WAITING, self.requests_handled])

    def _busy(self):
        self._child_conn.send([pfe.BUSY, self.requests_handled])

    def _error(self, msg=None):
        self.error = msg
        self._child_conn.send([pfe.EXITING_ERROR, str(msg)])

    def _handled_max_requests(self):
        self._child_conn.send([pfe.EXITING_MAX, ''])

    def process_request(self):
        self.log('im do work')


Server().run()