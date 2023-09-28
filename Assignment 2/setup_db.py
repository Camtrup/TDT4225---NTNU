import DbConnector
from tabulate import tabulate
from datetime import datetime as dt
import os;

class DBSetup:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor


    def create_tabel_User(self):
        query = """CREATE TABLE IF NOT EXISTS User (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY)
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_table_Activity(self):
        query = """CREATE TABLE IF NOT EXISTS Activity (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    user_id INT NOT NULL,
                    transportation_mode VARCHAR(255),
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    FOREIGN KEY (user_id) REFERENCES User(id))
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_table_TrackPoint(self):
        query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    activity_id INT NOT NULL,
                    lat DOUBLE,
                    lon DOUBLE,
                    altitude INT,
                    date DATETIME,
                    FOREIGN KEY (activity_id) REFERENCES Activity(id))
                """
        self.cursor.execute(query)
        self.db_connection.commit()


    def insert_data(self):
        progress = 0
        users = os.listdir("dataset/dataset/Data")
        users_with_labels = open("dataset/dataset/labeled_ids.txt", "r").read().split("\n")

        activity_query = """INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)"""
        trackpoint_query = """INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date) VALUES (%s, %s, %s, %s, %s)"""

        date_format = '%Y-%m-%d %H:%M:%S'

        users.sort()

        for user in users:
            activities = []
            trackpoints = []
            query = "INSERT INTO User (id) VALUES (NULL)"

            self.cursor.execute(query)

            user_id = self.cursor.lastrowid

            for trajectory in os.listdir("dataset/dataset/Data/" + user + "/Trajectory"):
                with open("dataset/dataset/Data/" + user + "/Trajectory/" + trajectory) as file:
                    lines = file.readlines()
                    lines = lines[6:]

                    if len(lines) >= 2500:
                        continue

                    start_line = lines[0].strip().split(",")
                    end_line = lines[-1].strip().split(",")

                    start_date_time_raw = start_line[5] + " " + start_line[6]
                    end_date_time_raw = end_line[5] + " " + end_line[6]

                    start_date_time = dt.strptime(start_date_time_raw, date_format)
                    end_date_time = dt.strptime(end_date_time_raw, date_format)
                    

                    transportation_mode = "NULL"

                    if user in users_with_labels:
                        with open("dataset/dataset/Data/" + user + "/labels.txt") as labels_file:
                            for line in labels_file.readlines()[1:]:
                                activity_start_raw = line.split("\t")[0].replace("/", "-")
                                activity_end_raw = line.split("\t")[1].replace("/", "-")

                                activity_start = dt.strptime(activity_start_raw, date_format)
                                activity_end = dt.strptime(activity_end_raw, date_format)

                                activity_mode = line.split("\t")[2].strip()

                                if start_date_time == activity_start and end_date_time == activity_end:
                                    transportation_mode = activity_mode
                                    break
                    
                    self.cursor.execute(activity_query, (user_id, transportation_mode, start_date_time, end_date_time))
                    activity_id = self.cursor.lastrowid

                    for line in lines:
                        line = line.strip().split(",")
                        lat = line[0]
                        lon = line[1]
                        altitude = line[3]
                        date_time_raw = line[5] + " " + line[6]
                        date_time = dt.strptime(date_time_raw, date_format)                    
                        self.cursor.execute(trackpoint_query, (activity_id, lat, lon, altitude, date_time))

            print("Progress: " + str(progress) + "/" + str(len(users)))
            progress += 1
            self.db_connection.commit()

    def drop_all_tables(self):
        query = "DROP TABLE IF EXISTS TrackPoint, Activity, User"
        self.cursor.execute(query)
        self.db_connection.commit()


    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

def main(args=None):
    program = None
    print(args)
    try:
        program = DBSetup()
        program.drop_all_tables()
        print("Dropped all tables")
        program.create_tabel_User()
        print("Created User table")
        program.create_table_Activity()
        print("Created Activity table")
        program.create_table_TrackPoint()
        print("Created TrackPoint table")
        program.insert_data()
        print("Inserted data")
        program.show_tables()
    except Exception as e:
        print("Error: ", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == "__main__":
    main()