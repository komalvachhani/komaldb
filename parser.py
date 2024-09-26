class GenZQueryParser:
    def __init__(self, database):
        self.db = database

    def parse(self, query):
        tokens = query.strip().replace(';', '').split()

        if not tokens:
            return "Dude, that's empty!"

        command = tokens[0].upper()

        if command == 'YOLO':
            if len(tokens) >= 4 and tokens[2] == '=':
                key = tokens[1]
                value = " ".join(tokens[3:])
                self.db.transactional_set(key, value)
                return f"Kk, set {key} = {value}"
            else:
                return "Bruh, fix your YOLO syntax!"

        elif command == 'WHATUP':
            if len(tokens) == 2:
                key = tokens[1]
                value = self.db.get(key)
                return value if value else f"Nah, {key} is NOT here!"
            else:
                return "Bruh, fix your WHATUP syntax!"

        elif command == 'PEACE':
            if len(tokens) == 2:
                key = tokens[1]
                self.db.delete(key)
                return f"Yeet! {key} is gone!"
            else:
                return "Bruh, fix your PEACE syntax!"

        elif command == 'LOCKIN':
            if len(tokens) == 3 and tokens[1].upper() == 'INTO' and tokens[2].upper() == 'DMS':
                self.db.begin_transaction()
                return "Commence the LOCKIN!"
            else:
                return "Bruh, fix the LOCKIN syntax!"

        elif command == 'NOCAP':
            if len(tokens) == 1:
                self.db.commit_transaction()
                return "Kk, transaction committed!"
            else:
                return "Bruh, fix your NOCAP syntax!"

        elif command == 'NAHJK':
            if len(tokens) == 1:
                self.db.rollback_transaction()
                return "Haha JK! We rolled it back!"
            else:
                return "Bruh, fix your NAHJK syntax!"

        elif command == 'SPILLTHETEA':
            if len(tokens) == 1:
                return "\n".join([f"{key}: {value}" for key, value in self.db.store.items()])
            else:
                return "Bruh, the tea is NOT spilling with that syntax!"

        else:
            return f"IDK what the heck {command} means, try again!"
