import socket

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
        self.connection = None
        self.addr = None

    def start(self):
        with socket.create_server(('localhost', 6379), reuse_port=True) as server:
            while True:
                self.connection, self.addr = server.accept()
                self.handle_single_connection()
                self.connection, self.addr = None, None

    def handle_single_connection(self):
        print(f"Connected to by {self.addr[0]}:{self.addr[1]}")
        with self.connection:
            while True:
                data: bytes = self.connection.recv(1024)
                if not data:
                    break
                command: str = RedisUtils.extract_command(data.decode('utf-8').split('\r\n'))
                print(f"Received command: {command}")
                match command:
                    case "ping":
                        response: str = RedisUtils.make_redis_simple_string("PONG")
                        print(f"Sending: {response}")
                self.connection.sendall(response.encode())


def main():
    server = RedisServer()
    server.start()


if __name__ == "__main__":
    main()
