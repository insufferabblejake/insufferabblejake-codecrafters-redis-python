import socketserver

HOST = "localhost"
LISTEN_PORT = 6379


class Protocol:
    def __init__(self):
        pass

    @staticmethod
    def make_redis_simple_string(s: str) -> str:
        return f"+{s}\r\n"


class RequestHandler(socketserver.StreamRequestHandler):

    def handle(self):
        # handle a single connection, which might send multiple commands
        response = ""
        # TODO this is the worlds worst parser!! Implement a proper one. But
        # at least I understand what's going on.
        flag = False
        while True:
            data = self.rfile.readline().rstrip()
            if data is None:
                break
            print(f"{self.client_address} wrote: {data} and {len(data)}")
            if data == b'ping':
                response = Protocol.make_redis_simple_string("PONG")
            if data == b'echo':
                flag = True
                continue
            if flag and data.decode().isalpha():
                response = Protocol.make_redis_simple_string(data.decode())
            self.wfile.write(response.encode())

    def execute_command_get_response(self, command, data):
        print(f"Received command {command} on {data.addr}")
        match command:
            case "ping":
                response: str = Protocol.make_redis_simple_string("PONG")
                print(f"Sending {response} on {data.addr}")
                # this will most likely need to be a separate call to write out the serialized response.
                data.outb += response.encode()


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def main():
    global server
    try:
        server = Server((HOST, LISTEN_PORT), RequestHandler)
        print(f"Serving on {server.server_address}")
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"Shutting down ..")
    finally:
        server.server_close()
        server.shutdown()


if __name__ == "__main__":
    main()
