import socket
import selectors
import types

HOST = "localhost"
LISTEN_PORT = 6379  # the port at which the listening server socket listens on for clients doing a connect()


class Protocol:
    def __init__(self):
        pass

    @staticmethod
    def make_redis_simple_string(s: str) -> str:
        return f"+{s}\r\n"

    @staticmethod
    def parse_client_request(data_list):
        # TODO this will have to parse the list
        return data_list[2]


class Server:
    def __init__(self):
        self.host = HOST
        self.listen_port = LISTEN_PORT
        self.sel = selectors.DefaultSelector()
        self.connection = None
        self.addr = None

    # Sets up the listening socket, registers it and starts the event loop
    # the listening socket is only registered for read events
    # and all that a listening socket does is produce 'client' sockets on the server
    # the serverside endpoint that is used to talk to clients.
    def start(self):
        print(f"Starting Redis Server.")
        listening_socket: socket.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        listening_socket.bind((self.host, self.listen_port))
        listening_socket.listen()
        print(f"Listening on {self.host}:{self.listen_port}")
        listening_socket.setblocking(False)
        self.sel.register(listening_socket, selectors.EVENT_READ, data=None)
        self._start_eventloop()

    def _start_eventloop(self):
        print(f"Starting event loop ...")
        try:
            while True:
                events = self.sel.select(timeout=None)
                for key, mask in events:
                    if key.data is None:    # listen_sock needs be accepted for new client conn,needs to be registered
                        self.accept_wrapper(key.fileobj)
                    else:                   # already registered client, needs to be serviced
                        self.connection_handler(key, mask)
        except KeyboardInterrupt:
            print("Exiting on user Ctrl-C")
        finally:
            self.sel.close()

    # Each new client socket that the listening_socket produces is registered with select() that can be
    # acted on when it is ready to be read from or written into.
    def accept_wrapper(self, listening_socket: object):
        client_socket, addr = listening_socket.accept()
        print(f"Accepted connection from {addr}")
        client_socket.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.sel.register(client_socket, events, data=data)

    # handles client requests by calling the protocol parser, executing command and writing out response
    def connection_handler(self, key, mask):
        client_socket, data = key.fileobj, key.data
        # print(f"Connected to by {data.addr[0]}:{data.addr[1]}")
        if mask & selectors.EVENT_READ:
            recv_data = client_socket.recv(1024)
            if recv_data:
                command: str = Protocol.parse_client_request(recv_data.decode('utf-8').split('\r\n'))
                self.execute_command_get_response(command, data)
            else:
                print(f"Closing connection to {data.addr}")
                self.sel.unregister(client_socket)
                client_socket.close()
        if mask & selectors.EVENT_WRITE:
            if data.outb:
                sent: int = client_socket.send(data.outb)
                print(f"Sent {sent} bytes to {data.addr}")
                # get rid of data already sent
                data.outb = data.outb[sent:]

    def execute_command_get_response(self, command, data):
        print(f"Received command {command} on {data.addr}")
        match command:
            case "ping":
                response: str = Protocol.make_redis_simple_string("PONG")
                print(f"Sending {response} on {data.addr}")
                # this will most likely need to be a separate call to write out the serialized response.
                data.outb += response.encode()


def main():
    server = Server()
    server.start()


if __name__ == "__main__":
    main()
