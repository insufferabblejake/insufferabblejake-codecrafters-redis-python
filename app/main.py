from dataclasses import dataclass
import socketserver
from typing import Callable, IO, List, Dict
import logging
import time

logger = logging.getLogger(__name__)

HOST = "localhost"
LISTEN_PORT = 6379
FIRST_BYTE = 1
UTF = 'utf-8'
REDIS_DELIMITER_LEN = 2
REDIS_DELIMITER = "\r\n"


@dataclass(frozen=True)
class Key:
    key: str


@dataclass
class Value:
    value: str
    expiry_time: float


Store: Dict[Key, Value] = {}


class Disconnect(Exception):
    pass


class CommandError(Exception):
    pass


class Protocol:
    def __init__(self):
        pass

    @staticmethod
    def make_redis_simple_string(s: str) -> str:
        return f"+{s}\r\n"

    @staticmethod
    def get_redis_null_string() -> str:
        return "$-1\r\n"

    def handle_request(self, socket_file: IO):
        header_byte = socket_file.read(FIRST_BYTE)
        if not header_byte:
            raise Disconnect
        try:
            header = header_byte.decode(UTF)
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

    def handle_array(self, socket_file: IO) -> List:
        logger.debug(f"In {self.handle_array.__name__}")
        array_len = int(socket_file.readline().rstrip())
        logger.debug(f"Got array of len {array_len}")
        return [self.handle_request(socket_file) for _ in range(array_len)]

    def handle_simple_string(self):
        logger.debug(f"{self.handle_simple_string.__name__}")

    def handle_string(self, socket_file: IO) -> str:
        logger.debug(f"{self.handle_string.__name__}")
        length = int(socket_file.readline().rstrip())
        logger.debug(f"Got string of len {length}")
        length += REDIS_DELIMITER_LEN
        # read length string, including the ending \r\n
        return socket_file.read(length)[:-REDIS_DELIMITER_LEN].decode(UTF)

    def handle_error(self):
        logger.debug(f"{self.handle_error.__name__}")
        raise NotImplemented

    def handle_integer(self):
        logger.debug(f"{self.handle_integer.__name__}")
        raise NotImplemented


class RequestHandler(socketserver.StreamRequestHandler):
    def __init__(self, request, client_address, rserver):
        self._protocol = Protocol()
        super().__init__(request, client_address, rserver)

    @staticmethod
    def _exec_ping() -> str:
        return Protocol.make_redis_simple_string("PONG")

    @staticmethod
    def _exec_echo(data: str) -> str:
        return Protocol.make_redis_simple_string(data)

    @staticmethod
    def _exec_set(data: List) -> str:
        # set key value px exp
        key = Key(data[1])
        val = data[2]
        expiry_time: float = 0.0
        if len(data) == 5 and data[3].upper() == "PX":
            expiry_time = time.time() + float(data[4]) / 1000
        value = Value(val, expiry_time)
        logger.debug(f"Inserting {key}:{value}")
        response: str = ""
        try:
            Store[key] = value
        except Exception:
            raise CommandError(f"Unable to insert {key}:{value}")
        else:
            response = Protocol.make_redis_simple_string("OK")
        finally:
            return response

    @staticmethod
    def _exec_get(data: List) -> str:
        response: str = ""
        key = Key(data[1])
        if key in Store:
            value: Value = Store.get(key)
            logger.debug(f"{value}")
            curr_time: float = time.time()
            logger.debug(f"Curr time {curr_time}")
            if value.expiry_time == 0:
                response = Protocol.make_redis_simple_string(value.value)
            elif value.expiry_time != 0 and curr_time < value.expiry_time:
                response = Protocol.make_redis_simple_string(value.value)
            else:
                response = Protocol.get_redis_null_string()
                del Store[key]
        return response

    def handle(self):
        logger.debug(f"In handler {self.client_address}")
        while True:
            try:
                data = self._protocol.handle_request(self.rfile)
            except Disconnect:
                print(f"Client went away")
                break

            try:
                if not isinstance(data, list) and not isinstance(data, str):
                    raise CommandError(f"Parsing Error {data}")
                resp = self.execute_command_get_response(data)
            except CommandError:
                raise CommandError("Unknown or unimplemented")

            self.wfile.write(resp.encode())

    def execute_command_get_response(self, data: List | str) -> str:
        if not isinstance(data, list):
            data = data.split()
        logger.debug(f"In {self.execute_command_get_response.__name__}, Got data: {data}")
        command = data[0].upper()
        logger.debug(f"Command: {command}")
        match command:
            case "PING":
                response = RequestHandler._exec_ping()
            case "ECHO":
                response = RequestHandler._exec_echo(data[1].rstrip())
            case "SET":
                response = RequestHandler._exec_set(data)
            case "GET":
                response = RequestHandler._exec_get(data)
            case _:
                raise CommandError
        logger.debug(f"Sending response: {response}")
        return response


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def main():
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)
    server = Server((HOST, LISTEN_PORT), RequestHandler)
    logger.info(f"Serving on {server.server_address}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info(f"Shutting down ..")
    finally:
        server.server_close()
        server.shutdown()


if __name__ == "__main__":
    main()
