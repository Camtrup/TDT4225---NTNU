from collections import defaultdict
from datetime import timedelta

from DbConnector import DbConnector
from haversine import Unit, haversine


class Queries:
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db

    ## Query 1: Find the number of users, activities and trackpoints in the dataset.
    def q1(self):
        print("Users:", self.db["User"].count_documents({}))
        print("Activities:", self.db["Activity"].count_documents({}))

    #    print(
    #       "TrackPoints:", self.db["TrackPoint"].count_documents({})
    #   )  # This takes a long time to run

    ## Query 2: Find the average number of activities per user
    def q2(self):
        a = self.db["User"].aggregate(
            [
                {
                    "$lookup": {
                        "from": "Activity",
                        "localField": "_id",
                        "foreignField": "user_id",
                        "as": "activities",
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "activities": 1,
                        "count": {"$size": "$activities"},
                    }
                },
                {"$group": {"_id": "null", "avg": {"$avg": "$count"}}},
            ]
        )

        print("Average number of activities per user:", list(a)[0]["avg"])

    ## Query 3: Find the top 20 users with the highest number of activities.
    def q3(self):
        a = self.db["User"].aggregate(
            [
                {
                    "$lookup": {
                        "from": "Activity",
                        "localField": "_id",
                        "foreignField": "user_id",
                        "as": "activities",
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "activities": 1,
                        "count": {"$size": "$activities"},
                    }
                },
                {"$sort": {"count": -1}},
                {"$limit": 20},
            ]
        )
        for i in a:
            print(
                "User",
                i["_id"],
                "has",
                i["count"],
                "activities",
            )

    ## Query 4: Find all users who have taken a taxi
    def q4(self):
        a = self.db["Activity"].aggregate(
            [
                {"$match": {"transportation_mode": "taxi"}},
                {"$group": {"_id": "$user_id"}},
            ]
        )
        for i in sorted(a, key=lambda x: x["_id"]):
            print("User", i["_id"])

    ## Query 5: Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null.
    def q5(self):
        a = self.db["Activity"].aggregate(
            [
                {"$match": {"transportation_mode": {"$ne": "NULL"}}},
                {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}},
            ]
        )
        for i in a:
            print(i["_id"], i["count"])

    ## Query 6:
    # a) Find the year with the most activities.
    # b) Is this also the year with most recorded hours?
    def q6(self):
        a = self.db["Activity"].aggregate(
            [
                {
                    "$project": {
                        "_id": 1,
                        "user_id": 1,
                        "transportation_mode": 1,
                        "start_date_time": 1,
                        "end_date_time": 1,
                        "duration": {
                            "$subtract": ["$end_date_time", "$start_date_time"]
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "user_id": 1,
                        "transportation_mode": 1,
                        "start_date_time": 1,
                        "end_date_time": 1,
                        "year": {"$year": "$start_date_time"},
                        "duration": 1,
                    }
                },
                {"$group": {"_id": "$year", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 1},
            ]
        )
        for i in a:
            print("Year with most activities:", i["_id"])

        b = self.db["Activity"].aggregate(
            [
                {
                    "$project": {
                        "_id": 1,
                        "user_id": 1,
                        "transportation_mode": 1,
                        "start_date_time": 1,
                        "end_date_time": 1,
                        "duration": {
                            "$subtract": ["$end_date_time", "$start_date_time"]
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "user_id": 1,
                        "transportation_mode": 1,
                        "start_date_time": 1,
                        "end_date_time": 1,
                        "year": {"$year": "$start_date_time"},
                        "duration": 1,
                    }
                },
                {"$group": {"_id": "$year", "count": {"$sum": "$duration"}}},
                {"$sort": {"count": -1}},
                {"$limit": 1},
            ]
        )
        for i in b:
            print("Year with most hours:", i["_id"])

    # Query 7: Find the total distance (in km) walked in 2008, by user with id=112.
    # use TrackPoint.date_time to determine if the activity is in 2008
    def q7(self):
        print("starting query")
        a = self.db["Activity"].aggregate(
            [
                {
                    "$match": {"user_id": "112", "transportation_mode": "walk"},
                },
                # Get all trackpoints for each activity
                {
                    "$lookup": {
                        "from": "TrackPoint",
                        "localField": "_id",
                        "foreignField": "activity_id",
                        "as": "trackpoints",
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "user_id": 1,
                        "transportation_mode": 1,
                        "start_date_time": 1,
                        "end_date_time": 1,
                        "trackpoints": 1,
                        "date_time": {"$year": "$start_date_time"},
                    }
                },
            ]
        )

        def get_distance(trackpoints):
            distance = 0
            for i in range(len(trackpoints) - 1):
                distance += haversine(
                    (trackpoints[i]["lat"], trackpoints[i]["lon"]),
                    (trackpoints[i + 1]["lat"], trackpoints[i + 1]["lon"]),
                    unit=Unit.KILOMETERS,
                )
            return distance

        total_distance = 0

        for i in a:
            if i["date_time"] == 2008:
                total_distance += get_distance(i["trackpoints"])
            print(
                "User",
                i["user_id"],
                "has walked",
                get_distance(i["trackpoints"]),
                "km in 2008",
                i["date_time"],
            )
        print("Total distance walked in 2008:", total_distance, "km")

    # Query 8: Find the top 20 users who have gained the most altitude meters.
    # Output should be a field with (id, total meters gained per user).
    # Remember that some altitude-values are invalid
    # Tip: ∑ (𝑡𝑝 𝑛. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒 − 𝑡𝑝 𝑛−1. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒), 𝑡𝑝 𝑛. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒 > tp 𝑛−1. 𝑎𝑙𝑡𝑖𝑡𝑢𝑑𝑒
    def q8(self):
        pipeline = [
            {
                "$lookup": {
                    "from": "TrackPoint",
                    "localField": "_id",
                    "foreignField": "activity_id",
                    "as": "trackpoints",
                }
            },
            {"$unwind": "$trackpoints"},
            {"$match": {"trackpoints.altitude": {"$ne": -777}}},
            {
                "$group": {
                    "_id": "$user_id",
                    "total_altitude_gained": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$trackpoints.altitude", "-777"]},
                                0,
                                {
                                    "$subtract": [
                                        {"$toDouble": "$trackpoints.altitude"},
                                        0.0,
                                    ]
                                },
                            ]
                        }
                    },
                }
            },
            {"$sort": {"total_altitude_gained": -1}},
            {"$limit": 20},
        ]

        # Execute the aggregation
        result = list(self.db["Activity"].aggregate(pipeline))

        print("Top 20 users who have gained the most altitude meters:")
        for i in result:
            print("User", i["_id"], "has gained", i["total_altitude_gained"], "meters")

    # Query 9: Find the top 15 users who have gained the most altitude meters.
    # Invalid altitude values must be ignored. They are represented as -777 in the database.
    def q9(self):
        pass

    # Query 10: Find the users that have traveled the longest total distance in one day for each
    # transportation mode.
    def q10(self):
        pass

    # Query 11: Find all users who have invalid activities, and the number of invalid activities per user.
    # An invalid activity is an activity with consecutive trackpoints where the timestamps deviate with
    # at least 5 minutes.
    def q11(self):
        pass

    # Query 12: Find all users who have registered transportation_mode and their most used
    # transportation_mode. The answer should be on format (user_id,
    # most_used_transportation_mode) sorted on user_id.
    # Some users may have the same number of activities tagged with e.g. walk
    # and car. In this case it is up to you to decide which transportation mode
    # to include in your answer (choose one).
    # Do not count the rows where the mode is null.
    def q12(self):
        pass


def main():
    q = None
    try:
        q = Queries()
        print("\n\nQuery 1:")
        q.q1()
        print("\n\nQuery 2:")
        q.q2()
        print("\n\nQuery 3:")
        q.q3()
        print("\n\nQuery 4:")
        q.q4()
        print("\n\nQuery 5:")
        q.q5()
        print("\n\nQuery 6:")
        q.q6()
        print("\n\nQuery 7:")
        #  q.q7()
        print("\n\nQuery 8:")
        q.q8()
        print("\n\nQuery 9:")
        q.q9()
        print("\n\nQuery 10:")
        q.q10()
        print("\n\nQuery 11:")
        q.q11()
        print("\n\nQuery 12:")
        q.q12()

    except Exception as e:
        print("Error: ", e)
    finally:
        if q:
            q.connection.close_connection()


if __name__ == "__main__":
    main()
