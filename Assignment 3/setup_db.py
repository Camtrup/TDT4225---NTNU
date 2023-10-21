import os
from datetime import datetime as dt

from DbConnector import DbConnector
from tqdm import tqdm


class DBSetup:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_collection(self, collection_name):
        try:
            self.db.create_collection(collection_name)
            print("Created collection:", collection_name)
        except Exception as e:
            print("ERROR: Failed to create collection:", e)

    def delete_collection(self, collection_name):
        try:
            self.db.drop_collection(collection_name)
            print("Deleted collection:", collection_name)
        except Exception as e:
            print("ERROR: Failed to delete collection:", e)

    def create_tables(self):
        self.create_collection("User")
        self.create_collection("Activity")
        self.create_collection("TrackPoint")

    def delete_tables(self):
        self.delete_collection("User")
        self.delete_collection("Activity")
        self.delete_collection("TrackPoint")

    def instert_activity(
        self, user_id, transportation_mode, start_date_time, end_date_time
    ):
        return self.db.Activity.insert_one(
            {
                "user_id": user_id,
                "transportation_mode": transportation_mode,
                "start_date_time": start_date_time,
                "end_date_time": end_date_time,
            }
        ).inserted_id

    def instert_trackpoint(self, activity_id, lat, lon, altitude, date_time):
        return self.db.TrackPoint.insert_one(
            {
                "activity_id": activity_id,
                "lat": lat,
                "lon": lon,
                "altitude": altitude,
                "date_time": date_time,
            }
        ).inserted_id

    def insert_user(self, user_id, has_labels):
        return self.db.User.insert_one(
            {"_id": user_id, "has_labels": has_labels}
        ).inserted_id

    def insert_data(self):
        users = os.listdir("dataset/Data")
        users_with_labels = open("dataset/labeled_ids.txt", "r").read().split("\n")

        date_format = "%Y-%m-%d %H:%M:%S"

        users.sort()
        num_users = len(users)
        for i in tqdm(range(num_users)):
            user = users[i]
            if user[0] == ".":
                continue

            # activities = []
            # trackpoints = []

            has_labels = user in users_with_labels

            user_id = self.insert_user(user, has_labels)

            for trajectory in os.listdir("dataset/Data/" + user + "/Trajectory"):
                with open("dataset/Data/" + user + "/Trajectory/" + trajectory) as f:
                    lines = f.readlines()[6:]

                    if len(lines) >= 2500:
                        continue

                    start_line = lines[0].strip().split(",")
                    end_line = lines[-1].strip().split(",")

                    start_date_time_raw = start_line[5] + " " + start_line[6]
                    end_date_time_raw = end_line[5] + " " + end_line[6]

                    start_date_time = dt.strptime(start_date_time_raw, date_format)
                    end_date_time = dt.strptime(end_date_time_raw, date_format)

                    transportation_mode = "NULL"
                    if has_labels:
                        with open(
                            "dataset/Data/" + user + "/labels.txt"
                        ) as labels_file:
                            for line in labels_file.readlines()[1:]:
                                activity_start_raw = line.split("\t")[0].replace(
                                    "/", "-"
                                )
                                activity_end_raw = line.split("\t")[1].replace("/", "-")

                                activity_start = dt.strptime(
                                    activity_start_raw, date_format
                                )
                                activity_end = dt.strptime(
                                    activity_end_raw, date_format
                                )

                                activity_mode = line.split("\t")[2].strip()

                                if (
                                    start_date_time == activity_start
                                    and end_date_time == activity_end
                                ):
                                    transportation_mode = activity_mode
                                    break
                    activity_id = self.instert_activity(
                        user_id, transportation_mode, start_date_time, end_date_time
                    )

                    for line in lines:
                        line = line.strip().split(",")
                        lat = line[0]
                        lon = line[1]
                        altitude = line[3]
                        date_time_raw = line[5] + " " + line[6]
                        date_time = dt.strptime(date_time_raw, date_format)
                        self.instert_trackpoint(
                            activity_id,
                            lat,
                            lon,
                            altitude,
                            date_time,
                        )
        print("Inserted data")


def main():
    inp = input("Are you sure you want to delete and setup all data? (y/n): ")
    if inp.lower() == "y":
        db_setup = DBSetup()
        db_setup.delete_tables()
        db_setup.create_tables()
        db_setup.insert_data()
    else:
        print("Exiting...")


if __name__ == "__main__":
    main()
