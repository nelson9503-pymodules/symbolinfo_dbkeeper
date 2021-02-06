from . import mysqlite
from datetime import datetime


class DBKeeper:

    def __init__(self, folder_path: str):
        """
        Symbol Information Database Keeper manage the symbol information data.
        Keeper will save the .db in the folder_path.
        """
        self.db_path = folder_path + "/symbolinfo.db"
        self.__initialize()

    def __initialize(self):
        self.db = mysqlite.DB(self.db_path)
        if not "master" in self.db.listTB():
            self.master = self.db.createTB("master", "symbol", "CHAR(100)")
            self.master.addCol("short_name", "CHAR(300)")
            self.master.addCol("long_name", "CHAR(500)")
            self.master.addCol("type", "CHAR(20)")
            self.master.addCol("market", "CHAR(2)")
            self.master.addCol("sector", "CHAR(100)")
            self.master.addCol("industry", "CHAR(100)")
            self.master.addCol("shares_outstanding", "BIGINT")
            self.master.addCol("market_cap", "BIGINT")
            self.master.addCol("fin_currency", "CHAR(10)")
            self.master.addCol("enable", "BOOLEAN")
        else:
            self.master = self.db.TB("master")
        self.mastertb = self.master.query()

    def update(self, symbol: dict, data: dict):
        if not symbol in self.mastertb:
            self.mastertb[symbol] = {}
        for item in data:
            self.mastertb[symbol][item] = data[item]
        self.master.update(self.mastertb)
        self.db.commit()

    def query(self, sql_condition: str = "") -> dict:
        tb = self.master.query("*", sql_condition)
        return tb
