import socketserver
import typing
from typing import Callable
import logging

logger = logging.getLogger(__name__)

HOST = "localhost"
LISTEN_PORT = 6379

Store: typing.Dict[str, str] = {}


class Disconnect(Exception):
    pass


class CommandError(Exception):
    pass


class Protocol:
    def __init__(self):
        pass

    def handle_request(self, socket_file: typing.IO):
        header_byte = socket_file.read(1)
        if not header_byte:
            raise Disconnect
        try:
            header = header_byte.decode('utf-8')
            logger.debug(f"Got header_byte: {header_byte}")
            handler = self.get_handlers(header)
            return handler(socket_file)
        except CommandError:
            raise CommandError('Bad request')

    def get_handlers(self, first_byte: str) -> Callable:
        match first_byte:
            case '*':
                return self.handle_array
            case '$':
                return self.handle_string
            case '+':
                return self.handle_simple_string
            case '-':
                return self.handle_error
            case ':':
                return self.handle_integer
            case _:
                raise CommandError()

    def handle_array(self, socket_file: typing.IO) -> typing.List:
        logger.debug(f"In {self.handle_array.__name__}")
        len_str = socket_file.readline().rstrip()
        array_len = int(len_str)
        logger.debug(f"Got array of len {array_len}")
        data = []
        for _ in range(array_len):
            data.append(self.handle_request(socket_file))
        return data

    def handle_simple_string(self):
        logger.debug(f"{self.handle_simple_string.__name__}")

    def handle_string(self, socket_file: typing.IO) -> str:
        logger.debug(f"{self.handle_string.__name__}")
        length = int(socket_file.readline().rstrip())
        logger.debug(f"Got string of len {length}")
        # read length string, including the ending \r\n
        return socket_file.read(length + 2)[:-2].decode('utf-8')

    def handle_error(self):
        logger.debug(f"{self.handle_error.__name__}")

    def handle_integer(self):
        logger.debug(f"{self.handle_integer.__name__}")

    @staticmethod
    def make_redis_simple_string(s: str) -> str:
        return f"+{s}\r\n"


class RequestHandler(socketserver.StreamRequestHandler):
    def __init__(self, request, client_address, rserver):
        self._protocol = Protocol()
        super().__init__(request, client_address, rserver)

    def handle(self):
        logger.debug(f"In handler {self.client_address}")
        while True:
            try:
                data = self._protocol.handle_request(self.rfile)
            except Disconnect:
                print(f"Client went away ")
                break

            try:
                resp = self.execute_command_get_response(data)
            except CommandError:
                raise CommandError("Unknown or unimplemented")

            self.wfile.write(resp.encode())

    def execute_command_get_response(self, data: typing.List | str) -> str:
        if not isinstance(data, list):
            data = data.split()
        logger.debug(f"In {self.execute_command_get_response.__name__}, Got data: {data}")
        command = data[0].upper()
        print(f"Command: {command}")
        response = ''
        match command:
            case "PING":
                response: str = Protocol.make_redis_simple_string("PONG")
            case "ECHO":
                echo_data = data[1]
                response = Protocol.make_redis_simple_string(echo_data.rstrip())
            case _:
                raise CommandError
        return response


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def main():
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)
    server = Server((HOST, LISTEN_PORT), RequestHandler)
    try:
        logger.info(f"Serving on {server.server_address}")
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info(f"Shutting down ..")
    finally:
        server.server_close()
        server.shutdown()


if __name__ == "__main__":
    main()
