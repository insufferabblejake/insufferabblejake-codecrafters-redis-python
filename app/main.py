import socket


def main():
    # You can use print statements as follows for debugging, they'll be
    # visible when running tests.
    print("Logs from your program will appear here!")

    with socket.create_server(('localhost', 6379), reuse_port=True) as server:
        while True:
            conn, addr = server.accept()


if __name__ == "__main__":
    main()
