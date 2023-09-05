from contextlib import contextmanager
import vertica_python


class VertConnect:
    def __init__(self, host: str, user: str, password: str, port: int) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.port = port


    @contextmanager
    def connection(self):
        conn = vertica_python.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port
        )
        try:
            yield conn
            conn.commit()
            print('commited')
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
