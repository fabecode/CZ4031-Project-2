from preprocessing import Database


def main():
    database = Database()
    database.query("...")
    database.closeConnection()


if __name__ == '__main__':
    main()
