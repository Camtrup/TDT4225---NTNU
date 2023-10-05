from DbConnector import DbConnector
from datetime import timedelta
from haversine import haversine, Unit

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
    def q9(self):
        query = """SELECT Sub.UserID, Sub.AltitudeGained 
            FROM ( 
                SELECT Activity.user_id AS userID, 
                       SUM(CASE WHEN TP1.altitude != -777 AND TP2.altitude != -777 
                       THEN (TP2.altitude - TP1.altitude) * 0.0003048 ELSE 0 END) AS AltitudeGained 
                FROM   TrackPoint AS TP1 INNER JOIN TrackPoint AS TP2 ON TP1.activity_id=TP2.activity_id AND 
                       TP1.id+1 = TP2.id INNER JOIN Activity ON Activity.id = TP1.activity_id AND Activity.id = TP2.activity_id 
                WHERE  TP2.altitude > TP1.altitude 
                GROUP  BY Activity.user_id 
                ) AS Sub 
            ORDER BY AltitudeGained DESC LIMIT 15
            """

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Top 15 users who have gained the most altitude meters:")
        for row in result:
            print("User: " + str(row[0]) + ", Altitude gain: " + str(row[1]))
    

    # Query 10: Find the users that have traveled the longest total distance in one day for each
    # transportation mode.
    def q10(self):
        query = """SELECT transportation_mode, user_id, SUM(distance) AS total_distance
            FROM (
                SELECT transportation_mode, user_id, activity_id, 
                    SUM(ST_Distance_Sphere(POINT(t1.lat, t1.lon), POINT(t2.lat, t2.lon))) AS distance
                FROM Activity, TrackPoint AS t1, TrackPoint AS t2
                WHERE Activity.id = t1.activity_id AND Activity.id = t2.activity_id
                    AND DATE(t1.date) = DATE(t2.date)
                    AND t1.date < t2.date
                GROUP BY transportation_mode, user_id, activity_id
            ) AS distances
            GROUP BY transportation_mode, user_id
            ORDER BY total_distance DESC
            """

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Users that have traveled the longest total distance in one day for each transportation mode:")
        for row in result:
            print("Transportation mode: " + str(row[0]) + ", User id: " + str(row[1]) + ", Total distance: " + str(row[2]))

    # Query 11: Find all users who have invalid activities, and the number of invalid activities per user.
    # An invalid activity is an activity with consecutive trackpoints where the timestamps deviate with
    # at least 5 minutes.
    def q11(self):
        query = """SELECT user_id, COUNT(*) AS invalid_activities
            FROM (
                SELECT user_id, activity_id, t1.date AS date1, t2.date AS date2
                FROM Activity, TrackPoint AS t1, TrackPoint AS t2
                WHERE Activity.id = t1.activity_id AND Activity.id = t2.activity_id
                    AND t1.date < t2.date
                    AND t1.date > DATE_SUB(t2.date, INTERVAL 5 MINUTE)
            ) AS invalid_activity
            GROUP BY user_id
            """

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Users who have invalid activities, and the number of invalid activities per user:")
        for row in result:
            print("User id: " + str(row[0]) + ", Number of invalid activities: " + str(row[1]))

    # Query 12: Find all users who have registered transportation_mode and their most used
    # transportation_mode. The answer should be on format (user_id,
    # most_used_transportation_mode) sorted on user_id.
    # Some users may have the same number of activities tagged with e.g. walk
    # and car. In this case it is up to you to decide which transportation mode
    # to include in your answer (choose one).
    # Do not count the rows where the mode is null.
    def q12(self):
        query = """SELECT user_id, transportation_mode
            FROM (
                SELECT user_id, transportation_mode, COUNT(*) AS count
                FROM Activity
                WHERE transportation_mode IS NOT NULL
                GROUP BY user_id, transportation_mode
                ORDER BY count DESC
            ) AS most_used
            GROUP BY user_id
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