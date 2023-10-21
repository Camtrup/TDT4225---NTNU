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

        print(
            "TrackPoints:", self.db["TrackPoint"].count_documents({})
        )  # This takes a long time to run

    ## Query 2: Find the average, minimum and maximum number of activities per user.
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
                {
                    "$group": {
                        "_id": None,
                        "avg": {"$avg": "$count"},
                        "min": {"$min": "$count"},
                        "max": {"$max": "$count"},
                    }
                },
            ]
        )
        for i in a:
            print("Average:", i["avg"])
            print("Minimum:", i["min"])
            print("Maximum:", i["max"])

    ## Query 3: Find the top 15 users having the largest number of activities.
    def q3(self):
        pass

    ## Query 4: Find all users who have taken a bus
    def q4(self):
        pass

    ## Query 5: List the top 10 users by their amount of different transportation modes
    def q5(self):
        pass

    ## Query 6: Find activities that are registered multiple times. You should find the query even if it gives zero result.
    def q6(self):
        pass

    # Query 7:
    # a) Find the number of users that have started an activity in one day and ended the activity the next day.
    # b) List the transportation mode, user id and duration for these activities.
    def q7(self):
        pass

    # Query 8: Find the number of users which have been close to each other in time and space.
    # Close is defined as the same space (50 meters) and for the same half minute (30seconds)
    def q8(self):
        pass

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
        q.q7()
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
