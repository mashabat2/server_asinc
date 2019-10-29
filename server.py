import asyncio

class ClientError(Exception):
    """Общий класс исключений клиента"""
    pass


class ClientSocketError(ClientError):
    """Исключение, выбрасываемое клиентом при сетевой ошибке"""
    pass


class ClientProtocolError(ClientError):
    """Исключение, выбрасываемое клиентом при ошибке протокола"""
    pass


dict = {}


class ClientServerProtocol(asyncio.Protocol):

    def connection_made(self, transport):
        self.transport = transport

    def process_data(self, data1):
        command, data = data1.split(' ', 1)
        data = data.strip()

        if command == 'put':
            try:
                metric, value, timestamp = data.split()
                if not metric in dict:
                    dict[metric] = []

                for timestamp2, value2 in dict[metric]:
                    if timestamp == timestamp2:
                        return 'ok\n\n'

                dict[metric].append((timestamp, value))
            except:
                print('Ошибка')

            return 'ok\n\n'

        elif command == 'get':
            result = 'ok\n'

            if data == '*':

                for key in dict:
                    dict[key].sort()
                    for timestamp, value in dict[key]:
                        result += '{} {} {}'.format(key, value, timestamp)
                        result += '\n'
            elif data in dict:
                for timestamp, value in dict[data]:
                    result += '{} {} {}'.format(data, value, timestamp)
                    result += '\n'

            result += '\n'
            return result
        else:
            return 'error\nwrong command\n\n'


    def data_received(self, data):
        try:
            message = data.decode().lower()
            resp = self.process_data(message)
            self.transport.write((resp).encode("utf-8"))
        except:
            raise ClientProtocolError



def run_server(host, port):

    loop = asyncio.get_event_loop()
    try:
        coro = loop.create_server(ClientServerProtocol, host, port)
    except:
        raise ClientSocketError()

    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == '__main__':
    run_server('127.0.0.1', 8888)
