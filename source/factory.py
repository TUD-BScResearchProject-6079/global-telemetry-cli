from typing import Optional

from data_loader import DataLoader
from data_processer import DataProcesser
from psycopg2.extensions import connection
from table_init import TableInitializer


class Factory:
    def __init__(self, conn: connection) -> None:
        self._conn = conn
        self._table_initializer: Optional[TableInitializer] = None
        self._data_loader: Optional[DataLoader] = None
        self._data_processer: Optional[DataProcesser] = None

    def get_table_initializer(self) -> TableInitializer:
        if self._table_initializer is None:
            self._table_initializer = TableInitializer(self._conn)
        return self._table_initializer

    def get_data_loader(self) -> DataLoader:
        if self._data_loader is None:
            self._data_loader = DataLoader(self._conn)
        return self._data_loader

    def get_data_processer(self) -> DataProcesser:
        if self._data_processer is None:
            self._data_processer = DataProcesser(self._conn)
        return self._data_processer
