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
        while True:
            data = self.rfile.readline().splitlines()
            if data is None or data == []:
                break
            print(f"{self.client_address} wrote: {data}")
            if b'ping' in data:
                response = Protocol.make_redis_simple_string("PONG")
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
