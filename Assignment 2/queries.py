from DbConnector import DbConnector


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
        query = 'SELECT avg(activity_count), min(activity_count), max(activity_count) FROM (SELECT count(*) as activity_count FROM Activity GROUP BY user_id) as activity_count'

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Average number of activities per user: " + str(result[0][0]))
        print("Minimum number of activities per user: " + str(result[0][1]))
        print("Maximum number of activities per user: " + str(result[0][2]))

    ## Query 3: Find the top 15 users having the largest number of activities.
    def q3(self):
        query = 'SELECT user_id, count(*) as activity_count FROM Activity GROUP BY user_id ORDER BY activity_count DESC LIMIT 15'

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
        query = 'SELECT user_id, COUNT(DISTINCT transportation_mode) as transportation_mode_count FROM Activity GROUP BY user_id ORDER BY transportation_mode_count DESC LIMIT 10'

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Top 10 users by their amount of different transportation modes:")
        for row in result:
            print("User: " + str(row[0]) + ", Number of different transportation modes: " + str(row[1]))

    ## Query 6: Find activities that are registered multiple times. You should find the query even if it gives zero result.
    def q6(self):
        query = 'SELECT activity_id, COUNT(*) as count FROM TrackPoint GROUP BY activity_id HAVING count > 1'

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        print("Activities that are registered multiple times:")
        for row in result:
            print("Activity: " + str(row[0]) + ", Number of times registered: " + str(row[1]))
    

    
    
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

    except Exception as e:
        print("Error: ", e)
    finally:
        if q:
            q.connection.close_connection()


if __name__ == "__main__":
    main()