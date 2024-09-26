import threading
import json
import logging
from collections import defaultdict

class ScalableDatabase:
    def __init__(self, filename='database.json', log_filename='db.log'):
        self.store = {}  # main storage for key-value pairs
        self.indexes = defaultdict(dict)  # indexes for fast lookups
        self.locks = defaultdict(threading.Lock)  # locks for each key
        self.transaction_logs = []  # logs for transactions
        self.filename = filename  # file to save/load the database
        self.log_filename = log_filename
        self.setup_logging()
        self.load()

    def setup_logging(self):
        self.logger = logging.getLogger('ScalableDatabaseLogger')
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(self.log_filename)
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def transactional_set(self, key, value):
        if self.transaction_logs:
            old_value = self.get(key)
            self.transaction_logs[-1].append((key, old_value))
        self.set(key, value)
        self.logger.info(f"Transactional set: {key} = {value}")

    def set(self, key, value):
        with self.locks[key]:
            self.store[key] = value
            self._update_indexes(key, value)
            self.save()
            self.logger.info(f"Set key '{key}' to value '{value}'")

    def get(self, key):
        with self.locks[key]:
            value = self.store.get(key, None)
            self.logger.info(f"Get key '{key}' returned '{value}'")
            return value

    def delete(self, key):
        with self.locks[key]:
            if key in self.store:
                del self.store[key]
                self._remove_from_indexes(key)
                self.save()
                self.logger.info(f"Deleted key '{key}'")

    def begin_transaction(self):
        self.transaction_logs.append([])
        self.logger.info("Transaction started")

    def commit_transaction(self):
        if not self.transaction_logs:
            raise Exception("No transaction started")
        self.transaction_logs.pop()  # commit removes last transaction log
        self.save()
        self.logger.info("Transaction committed")

    def rollback_transaction(self):
        if not self.transaction_logs:
            raise Exception("No transaction started")
        log = self.transaction_logs.pop()  # get last transaction log
        for action in reversed(log):
            key, old_value = action
            if old_value is None:
                self.delete(key)
            else:
                self.set(key, old_value)
        self.logger.info("Transaction rolled back")

    def _update_indexes(self, key, value):
        """
        Updates indexes based on the new value set for a key.
        """
        for index_key in self.indexes:
            if index_key in value:
                self.indexes[index_key][value[index_key]] = key

    def _remove_from_indexes(self, key):
        """
        Removes a key from all indexes.
        """
        for index_key in self.indexes:
            for indexed_value, indexed_key in list(self.indexes[index_key].items()):
                if indexed_key == key:
                    del self.indexes[index_key][indexed_value]

    def add_index(self, index_key):
        """
        Adds an index on a specific field of the value.
        """
        self.indexes[index_key] = {}
        for key, value in self.store.items():
            if index_key in value:
                self.indexes[index_key][value[index_key]] = key

    def search_by_index(self, index_key, search_value):
        """
        Searches for a key by its index value.
        """
        return self.indexes.get(index_key, {}).get(search_value)

    def save(self):
        """
        Saves the current state of the database to a file
        """
        with open(self.filename, 'w') as f:
            json.dump(self.store, f)
        self.logger.info("Database saved")

    def load(self):
        try:
            with open(self.filename, 'r') as f:
                self.store = json.load(f)
            self.logger.info("Database loaded")
        except FileNotFoundError:
            self.store = {}
            self.logger.info("No database file found, starting with an empty store")

    def __repr__(self):
        return f"ScalableDatabase(store={self.store})"

