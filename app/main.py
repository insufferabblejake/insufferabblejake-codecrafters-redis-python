import socket
import selectors
import types

HOST = "localhost"
PORT = 6379


class RedisUtils:
    def __init__(self):
        pass

    @staticmethod
    def make_redis_simple_string(s: str) -> str:
        return f"+{s}\r\n"

    @staticmethod
    def extract_command(data_list):
        # TODO this will have to parse the list
        return data_list[2]


class RedisServer:
    def __init__(self):
        self.host = HOST
        self.port = PORT
        self.sel = selectors.DefaultSelector()
        self.connection = None
        self.addr = None

    # Sets up the listening socket, registers it and starts the event loop
    # the listening socket is only registered for read events
    def start(self):
        print(f"Starting Redis Server.")
        listening_sock: socket.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        listening_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        listening_sock.bind((self.host, self.port))
        listening_sock.listen()
        print(f"Listening on {self.host}:{self.port}")
        listening_sock.setblocking(False)
        self.sel.register(listening_sock, selectors.EVENT_READ, data=None)
        self._start_eventloop()

    def _start_eventloop(self):
        print(f"Starting event loop ...")
        try:
            while True:
                events = self.sel.select(timeout=None)
                for key, mask in events:
                    if key.data is None:    # new client conn, needs to be registered
                        self._accept_wrapper(key.fileobj)
                    else:                   # already registered client, needs to be serviced
                        self._handle_single_connection(key, mask)
        except KeyboardInterrupt:
            print("Exiting on user Ctrl-C")
        finally:
            self.sel.close()

    # Each new client socket gets registered to be selected for later when ready
    def _accept_wrapper(self, sock: object):
        conn, addr = sock.accept()
        print(f"Accepted connection from {addr}")
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.sel.register(conn, events, data=data)

    def _handle_single_connection(self, key, mask):
        connection = key.fileobj
        data = key.data
        # print(f"Connected to by {data.addr[0]}:{data.addr[1]}")
        if mask & selectors.EVENT_READ:
            self._handle_client_read(connection, data)
        if mask & selectors.EVENT_WRITE:
            self._handle_client_write(connection, data)

    def _handle_client_read(self, connection, data):
        recv_data = connection.recv(1024)
        if recv_data:
            command: str = RedisUtils.extract_command(recv_data.decode('utf-8').split('\r\n'))
            print(f"Received command {command} on {data.addr}")
            match command:
                case "ping":
                    response: str = RedisUtils.make_redis_simple_string("PONG")
                    print(f"Sending {response} on {data.addr}")
                    data.outb += response.encode()
        else:
            print(f"Closing connection to {data.addr}")
            self.sel.unregister(connection)
            connection.close()

    def _handle_client_write(self, connection, data):
        # you can save data to be written out to the connection when it is ready to be written to
        # data gets accumulated in outb and each time it is ready to be sent, try a send().
        # this way we avoid having to call sendall()
        if data.outb:
            sent = connection.send(data.outb)
            # get rid of data already sent
            data.outb = data.outb[sent:]


def main():
    server = RedisServer()
    server.start()


if __name__ == "__main__":
    main()
