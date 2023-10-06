from DbConnector import DbConnector
from datetime import timedelta
from haversine import haversine, Unit
from collections import defaultdict

class Queries:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    ## Query 1: Find the number of users, activities and trackpoints in the dataset.
    def q1(self):
        query_user = 'SELECT count(*) FROM User'
        query_activity = 'SELECT count(*) FROM Activity'
        query_trackpoint = 'SELECT count(*) FROM TrackPoint'

        self.cursor.execute(query_user)
        user = self.cursor.fetchall()

        self.cursor.execute(query_activity)
        activity = self.cursor.fetchall()

        self.cursor.execute(query_trackpoint)
        trackpoint = self.cursor.fetchall()

        print("Number of users: " + str(user[0][0]))
        print("Number of activities: " + str(activity[0][0]))
        print("Number of trackpoints: " + str(trackpoint[0][0]))

    ## Query 2: Find the average, minimum and maximum number of activities per user.
    def q2(self):
        query = """SELECT avg(activity_count), min(activity_count), max(activity_count) 
        FROM (SELECT count(*) as activity_count 
        FROM Activity 
        GROUP BY user_id) as activity_count"""

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Average number of activities per user: " + str(result[0][0]))
        print("Minimum number of activities per user: " + str(result[0][1]))
        print("Maximum number of activities per user: " + str(result[0][2]))

    ## Query 3: Find the top 15 users having the largest number of activities.
    def q3(self):
        query = """SELECT user_id, count(*) as activity_count 
        FROM Activity GROUP BY user_id 
        ORDER BY activity_count DESC LIMIT 15"""

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Top 15 users having the largest number of activities:")
        for row in result:
            print("User: " + str(row[0]) + ", Number of activities: " + str(row[1]))

    ## Query 4: Find all users who have taken a bus
    def q4(self):
        query = 'SELECT DISTINCT user_id FROM Activity WHERE transportation_mode = "bus"'

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Users who have taken a bus:")
        for row in result:
            print("User: " + str(row[0]))

    
    ## Query 5: List the top 10 users by their amount of different transportation modes
    def q5(self):
        query = """SELECT user_id, COUNT(DISTINCT transportation_mode) as transportation_mode_count 
        FROM Activity 
        GROUP BY user_id 
        ORDER BY transportation_mode_count 
        DESC LIMIT 10"""

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Top 10 users by their amount of different transportation modes:")
        for row in result:
            print("User: " + str(row[0]) + ", Number of different transportation modes: " + str(row[1]))

    ## Query 6: Find activities that are registered multiple times. You should find the query even if it gives zero result.
    def q6(self):
        query = 'SELECT id FROM Activity GROUP BY id HAVING COUNT(*) > 1'

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Activities that are registered multiple times:")
        for row in result:
            print("Activity: " + str(row[0]) + ", Number of times registered: " + str(row[1]))


    # Query 7: 
    # a) Find the number of users that have started an activity in one day and ended the activity the next day. 
    # b) List the transportation mode, user id and duration for these activities.
    def q7(self):
        query = """SELECT transportation_mode, user_id, TIMESTAMPDIFF(MINUTE, start_date_time, end_date_time) as duration 
        FROM Activity 
        WHERE DATE(start_date_time) < DATE(end_date_time) 
        AND DATE(end_date_time) = DATE_ADD(DATE(start_date_time), INTERVAL 1 DAY)"""

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Users that have started an activity in one day and ended the activity the next day (duration is in minutes):")
        for row in result:
            print("Transportation mode: " + str(row[0]) + ", User id: " + str(row[1]) + ", Duration: " + str(row[2]))
    
    # Query 8: Find the number of users which have been close to each other in time and space.
    # Close is defined as the same space (50 meters) and for the same half minute (30seconds)
    def q8(self):
        query = """SELECT * FROM TrackPoint NATURAL JOIN Activity"""

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        been_close = set()

        delta = timedelta(seconds=30)
        # Index for each value
        # Activity_id = 1
        # Lat = 2
        # Lon = 3
        # Altitude = 4
        # Date_time = 5
        # User_id = 6
        for rowone in result:
            for rowtwo in result:
                if rowone[6] == rowtwo[6]:
                    continue
                if rowone[5] == rowtwo[5] + delta or rowone[5] == rowtwo[5] - delta:
                    if haversine((rowone[2], rowone[3]), (rowtwo[2], rowtwo[3]), unit=Unit.METERS) < 50:
                        been_close.add((rowone[6], rowtwo[6]))
                        been_close.add((rowtwo[6], rowone[6]))

        count = len(been_close) / 2

        print("Users which have been close to each other in time and space:")
        print("Number of users: " + str(count))

    # Query 9: Find the top 15 users who have gained the most altitude meters.
    # Invalid altitude values must be ignored. They are represented as -777 in the database.
    def q9(self):
        query = """SELECT user_id, SUM(altitude) * 0.0003048 AS altitude_sum
            FROM (
                SELECT user_id, activity_id, altitude
                FROM Activity, TrackPoint
                WHERE Activity.id = TrackPoint.activity_id
                    AND altitude != -777
            ) AS altitudes
            GROUP BY user_id
            ORDER BY altitude_sum DESC
            LIMIT 15
            """

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Top 15 users who have gained the most altitude meters:")
        for row in result:
            print("User id: " + str(row[0]) + ", Altitude sum: " + str(row[1]))

    # Query 10: Find the users that have traveled the longest total distance in one day for each
    # transportation mode. 
    def q10(self):
        # Hashmap for each transportation mode and a tuple with the user id and the distance
        # {transportation_mode: (user_id, distance)}
        transportation_mode_distance = defaultdict(lambda: (0, 0))

        query ="""SELECT id FROM User"""

        self.cursor.execute(query)
        users = self.cursor.fetchall()

        for user in users:
            query = """SELECT id, transportation_mode FROM Activity WHERE user_id = %s AND transportation_mode NOT LIKE 'NULL'"""

            self.cursor.execute(query, (user[0],))
            activities = self.cursor.fetchall()

            for activity in activities:
                query = """SELECT lat, lon, date FROM TrackPoint WHERE activity_id = %s"""

                self.cursor.execute(query, (activity[0],))
                trackpoints = self.cursor.fetchall()

                distance = 0
                # Add the distance between trackpoints on the same day
                for i in range(len(trackpoints) - 1):
                    if trackpoints[i][2].date() == trackpoints[i + 1][2].date():
                        distance += haversine((trackpoints[i][0], trackpoints[i][1]), (trackpoints[i + 1][0], trackpoints[i + 1][1]), unit=Unit.METERS)
                    else:
                        distance = 0;
                    
                    if distance > transportation_mode_distance[activity[1]][1]:
                        transportation_mode_distance[activity[1]] = (user[0], distance)

        print("Users that have traveled the longest total distance in one day for each transportation mode:")
        for key, value in transportation_mode_distance.items():
            print("Transportation mode: " + str(key) + ", User id: " + str(value[0]) + ", Distance: " + str(value[1]))


    # Query 11: Find all users who have invalid activities, and the number of invalid activities per user.
    # An invalid activity is an activity with consecutive trackpoints where the timestamps deviate with
    # at least 5 minutes.
    def q11(self):
        # query = """SELECT user_id, COUNT(DISTINCT a.id) AS number_of_invalid_activities
        #         FROM Activity a
        #         JOIN TrackPoint t1 ON a.id = t1.activity_id
        #         JOIN TrackPoint t2 ON a.id = t2.activity_id AND t1.id < t2.id
        #         WHERE TIMESTAMPDIFF(MINUTE, t1.date, t2.date) >= 5
        #         GROUP BY user_id;
        #     """

        # self.cursor.execute(query)
        # result = self.cursor.fetchall()

        # print("Users who have invalid activities, and the number of invalid activities per user:")
        # for row in result:
        #     print("User id: " + str(row[0]) + ", Number of invalid activities: " + str(row[1]))

        query = """SELECT id from User"""

        self.cursor.execute(query)
        users = self.cursor.fetchall()

        invalid_activities = defaultdict(int)

        for user in users:
            query = """SELECT id from Activity WHERE user_id = %s"""

            self.cursor.execute(query, (user[0],))
            activities = self.cursor.fetchall()

            for activity in activities:
                query = """SELECT date FROM TrackPoint WHERE activity_id = %s ORDER BY date"""

                self.cursor.execute(query, (activity[0],))
                trackpoints = self.cursor.fetchall()

                for i in range(len(trackpoints) - 1):
                    if trackpoints[i][0] + timedelta(minutes=5) < trackpoints[i + 1][0]:
                        invalid_activities[user[0]] += 1
                        break
        
        print("Users who have invalid activities, and the number of invalid activities per user:")
        for key, value in invalid_activities.items():
            print("User id: " + str(key) + ", Number of invalid activities: " + str(value))

    # Query 12: Find all users who have registered transportation_mode and their most used
    # transportation_mode. The answer should be on format (user_id,
    # most_used_transportation_mode) sorted on user_id.
    # Some users may have the same number of activities tagged with e.g. walk
    # and car. In this case it is up to you to decide which transportation mode
    # to include in your answer (choose one).
    # Do not count the rows where the mode is null.
    def q12(self):
        query = """WITH TransportationCounts AS (
                SELECT user_id, transportation_mode, COUNT(*) as count_mode
                FROM Activity
                WHERE transportation_mode NOT LIKE 'NULL'
                GROUP BY user_id, transportation_mode
            )

            SELECT user_id, transportation_mode
            FROM (
                SELECT user_id, transportation_mode, count_mode,
                    RANK() OVER(PARTITION BY user_id ORDER BY count_mode DESC) as rnk
                FROM TransportationCounts
            ) AS RankedModes
            WHERE rnk = 1;
            """

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Users who have registered transportation_mode and their most used transportation_mode:")
        for row in result:
            print("User id: " + str(row[0]) + ", Most used transportation mode: " + str(row[1]))
    

    
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