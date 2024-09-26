import sys
from PyQt5.QtWidgets import (QApplication, QWidget, QLabel, QLineEdit, QPushButton, 
                             QVBoxLayout, QHBoxLayout, QMessageBox, QTextEdit)
import json
from db import ScalableDatabase
from parser import GenZQueryParser

class DatabaseGUI(QWidget):
    def __init__(self):
        super().__init__()

        self.db = ScalableDatabase()
        self.parser = GenZQueryParser(self.db)

        self.initUI()

    def initUI(self):
        self.setWindowTitle('KomalDB')

        # Key entry
        self.key_label = QLabel('Key:')
        self.key_entry = QLineEdit()

        # Value entry
        self.value_label = QLabel('Value (JSON format or simple value):')
        self.value_entry = QLineEdit()

        # Buttons
        self.set_button = QPushButton('Set')
        self.set_button.clicked.connect(self.set_value)

        self.get_button = QPushButton('Get')
        self.get_button.clicked.connect(self.get_value)

        self.delete_button = QPushButton('Delete')
        self.delete_button.clicked.connect(self.delete_value)

        self.display_button = QPushButton('Display Database')
        self.display_button.clicked.connect(self.display_database)

        self.clear_button = QPushButton('Clear Database')
        self.clear_button.clicked.connect(self.clear_database)

        self.begin_transaction_button = QPushButton('Begin Transaction')
        self.begin_transaction_button.clicked.connect(self.begin_transaction)

        self.commit_transaction_button = QPushButton('Commit Transaction')
        self.commit_transaction_button.clicked.connect(self.commit_transaction)

        self.rollback_transaction_button = QPushButton('Rollback Transaction')
        self.rollback_transaction_button.clicked.connect(self.rollback_transaction)

        # Indexing section
        self.index_label = QLabel('Field to Index:')
        self.index_entry = QLineEdit()

        self.add_index_button = QPushButton('Add Index')
        self.add_index_button.clicked.connect(self.add_index)

        # Search by index
        self.search_index_label = QLabel('Search Field:')
        self.search_value_entry = QLineEdit()

        self.search_button = QPushButton('Search by Index')
        self.search_button.clicked.connect(self.search_by_index)

        # Query section
        self.query_label = QLabel('KQuery:')
        self.query_entry = QLineEdit()

        self.query_button = QPushButton('Submit')
        self.query_button.clicked.connect(self.execute_query)

        # Text widget to display database contents and search results
        self.display_text = QTextEdit()
        self.display_text.setReadOnly(True)

        # Layout setup
        layout = QVBoxLayout()

        # Row 1: Key and Value entry
        row1 = QHBoxLayout()
        row1.addWidget(self.key_label)
        row1.addWidget(self.key_entry)
        layout.addLayout(row1)

        row2 = QHBoxLayout()
        row2.addWidget(self.value_label)
        row2.addWidget(self.value_entry)
        layout.addLayout(row2)

        # Row 2: Set, Get, Delete buttons
        row3 = QHBoxLayout()
        row3.addWidget(self.set_button)
        row3.addWidget(self.get_button)
        row3.addWidget(self.delete_button)
        layout.addLayout(row3)

        # Row 3: Display, Clear database, Transactions
        row4 = QHBoxLayout()
        row4.addWidget(self.display_button)
        row4.addWidget(self.clear_button)
        layout.addLayout(row4)

        row5 = QHBoxLayout()
        row5.addWidget(self.begin_transaction_button)
        row5.addWidget(self.commit_transaction_button)
        row5.addWidget(self.rollback_transaction_button)
        layout.addLayout(row5)

        # Row 4: Indexing and Search
        row6 = QHBoxLayout()
        row6.addWidget(self.index_label)
        row6.addWidget(self.index_entry)
        row6.addWidget(self.add_index_button)
        layout.addLayout(row6)

        row7 = QHBoxLayout()
        row7.addWidget(self.search_index_label)
        row7.addWidget(self.search_value_entry)
        row7.addWidget(self.search_button)
        layout.addLayout(row7)

        # Row 5: Query and result display
        row8 = QHBoxLayout()
        row8.addWidget(self.query_label)
        row8.addWidget(self.query_entry)
        row8.addWidget(self.query_button)
        layout.addLayout(row8)

        layout.addWidget(self.display_text)

        self.setLayout(layout)

    def set_value(self):
        key = self.key_entry.text()
        value = self.value_entry.text()
        if key and value:
            try:
                try:
                    parsed_value = json.loads(value)
                except json.JSONDecodeError:
                    parsed_value = value  # Use value as a string if not JSON
                self.db.transactional_set(key, parsed_value)
                self.show_message("Success", f"Set key '{key}' to value '{parsed_value}'")
            except Exception as e:
                self.show_message("Error", f"Failed to set value: {str(e)}")
        else:
            self.show_message("Error", "Key and Value must not be empty")

    def get_value(self):
        key = self.key_entry.text()
        if key:
            value = self.db.get(key)
            if value:
                self.show_message("Result", f"Value for '{key}': {value}")
            else:
                self.show_message("Result", f"Key '{key}' not found")
        else:
            self.show_message("Error", "Key must not be empty")

    def delete_value(self):
        key = self.key_entry.text()
        if key:
            self.db.delete(key)
            self.show_message("Success", f"Deleted key '{key}'")
        else:
            self.show_message("Error", "Key must not be empty")

    def begin_transaction(self):
        self.db.begin_transaction()
        self.show_message("Transaction", "Transaction started")

    def commit_transaction(self):
        try:
            self.db.commit_transaction()
            self.show_message("Transaction", "Transaction committed")
        except Exception as e:
            self.show_message("Error", str(e))

    def rollback_transaction(self):
        try:
            self.db.rollback_transaction()
            self.show_message("Transaction", "Transaction rolled back")
        except Exception as e:
            self.show_message("Error", str(e))

    def add_index(self):
        field = self.index_entry.text()
        if field:
            self.db.add_index(field)
            self.show_message("Success", f"Index added on field '{field}'")
        else:
            self.show_message("Error", "Field to index must not be empty")

    def search_by_index(self):
        field = self.index_entry.text()
        search_value = self.search_value_entry.text()
        if field and search_value:
            key = self.db.search_by_index(field, search_value)
            if key:
                self.show_message("Result", f"Found key '{key}' for {field}='{search_value}'")
                self.display_text.append(f"Search result: {field}='{search_value}' found in key '{key}'\n")
            else:
                self.show_message("Result", f"No entry found for {field}='{search_value}'")
                self.display_text.append(f"No entry found for {field}='{search_value}'\n")
        else:
            self.show_message("Error", "Field and search value must not be empty")

    def display_database(self):
        self.display_text.clear()
        for key, value in self.db.store.items():
            self.display_text.append(f"{key}: {value}\n")

    def clear_database(self):
        self.db.store.clear()
        self.db.save()
        self.display_text.clear()
        self.show_message("Success", "Database cleared")

    def execute_query(self):
        query = self.query_entry.text()
        result = self.parser.parse(query)
        self.display_text.append(f"{result}\n")

    def show_message(self, title, message):
        QMessageBox.information(self, title, message)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    gui = DatabaseGUI()
    gui.show()
    sys.exit(app.exec_())
