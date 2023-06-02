import socket
# TODO add simple logging levels


HOST = "localhost"
PORT = 6379


def make_simple_string(s: str) -> str:
    # given a regular string, make it a simple string that follows
    # the redis protocol
    return f"+{s}\r\n"


class RedisServer:
    def __init__(self):
        self.host = HOST
        self.port = PORT
        self.connection = None
        self.addr = None

    def start(self):
        with socket.create_server(('localhost', 6379), reuse_port=True) as server:
            while True:
                self.connection, self.addr = server.accept()
                self.handle_single_connection()
                self.connection = None

    def handle_single_connection(self):
        with self.connection:
            print(f"Connected by {self.addr[0]}:{self.addr[1]}")
            response = make_simple_string("PONG").encode()
            # sendall() blocks and ties to send all the data you have, whereas send() might not.
            # apparently less error-prone to use sendall()
            self.connection.sendall(response)


def main():
    # You can use print statements as follows for debugging, they'll be
    # visible when running tests.
    print("Logs from your program will appear here!")
    server = RedisServer()
    server.start()


if __name__ == "__main__":
    main()
