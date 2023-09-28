import DbConnector

class Queries:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor


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
    

