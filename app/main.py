import socket
# TODO add simple logging levels


def make_simple_string(s: str) -> str:
    # given a regular string, make it a simple string that follows
    # the redis protocol
    return f"+{s}\r\n"


def main():
    # You can use print statements as follows for debugging, they'll be
    # visible when running tests.
    print("Logs from your program will appear here!")

    with socket.create_server(('localhost', 6379), reuse_port=True) as server:
        while True:
            conn, addr = server.accept()
            with conn:
                print(f"Connected by {addr}")
                response = make_simple_string("PONG").encode()
                # sendall() blocks and ties to send all the data you have, whereas send() might not.
                # apparently less error-prone to use sendall()
                conn.sendall(response)


if __name__ == "__main__":
    main()
