#!/usr/bin/env python
# -*- coding: utf-8 -*-

#Queries in Cypher embedded in python modules via py2neo library. Fetching collected twitter data in along with key attributes(text,hashtag,mention) and relationships from Neo4j DB


from py2neo import Graph
graph = Graph()

#Returns Tweets that contain a specific Hashtag along with the key features (No Retweet)
def get_tweets_hashtag(hashword):
    query= '''
        MATCH (u:User)-[:POSTS]->(n:Tweet)-[:TAGS]->(ht0:Hashtag {Word:{hashword}})
        WHERE NOT (n)-[:`RETWEET OF`]->()
        OPTIONAL MATCH (n:Tweet)-[:TAGS]->(ht)
        OPTIONAL MATCH (n:Tweet)-[:MENTIONS]->(mtus)
        RETURN COLLECT(DISTINCT ht.Word) AS hts,COLLECT(DISTINCT mtus.Screen_Name) AS mts,n.Text AS txt,n.ID AS idn,n.Date AS date,u.Screen_Name as sname
        '''
    tweets=graph.cypher.execute(query,hashword=hashword)
    return tweets
#Example:
    #hashtag='Oscars'
    # tweets=get_tweets_hashtag(hashtag)



#Returns Tweets that were posted between specific dates along with the key features (No Retweet)
def get_tweets_between_dates(start_date,end_date):
    query= '''
        MATCH (n:Tweet)-[:TAGS]->(ht0:Hashtag)
        WHERE NOT (n)-[:`RETWEET OF`]->()
		AND n.Date>{date1} AND n.Date<{date2}
        OPTIONAL MATCH (n:Tweet)-[:TAGS]->(ht)
        OPTIONAL MATCH (n:Tweet)-[:MENTIONS]->(mtus)
        RETURN COLLECT(DISTINCT ht.Word) AS hts,COLLECT(DISTINCT mtus.Screen_Name) AS mts,n.Text AS txt,n.ID AS idn,n.Date AS date
        '''
    tweets=graph.cypher.execute(query,date1=start_date,date2=end_date)
    return tweets
#Example:
    # start_date='2015-12-25'
    # end_date='2016-01-07'
    # tweets=get_tweets_between_dates(start_date,end_date)


#Returns most followed users in our Neo4j DB    
def get_top_users():
    query="""
    MATCH (u:User)
    RETURN u.Description AS Description,
               u.Tweets AS Tweets,
               u.Followers AS Followers,
               u.Friends AS Friends,
               u.Screen_Name AS Username,
               u.Image AS imglink
    ORDER BY u.Followers DESC
    LIMIT 20
    """
    users = graph.cypher.execute(query)
    return users  
    
    

#Returns most used hashtags in our Neo4j DB
def get_top_hashtags():
    query="""MATCH (t:Tweet)-[:TAGS]->(h) 
             RETURN h.Word as Hashtag, COUNT(t) as Times 
             ORDER BY Times DESC 
             LIMIT 20
    """
    return graph.cypher.execute(query)
 
#Returns most mentioned twitter users in our Neo4j DB
def get_most_mentioned_users():
        query="""
        MATCH (t:Tweet)-[:MENTIONS]->(u:User)
        RETURN u.Screen_Name AS Username, COUNT(t) AS Mentions
        ORDER BY Mentions DESC
        LIMIT 20
        """
        return graph.cypher.execute(query)
      
#Returns the most common pairs of hashtags in our Neo4j DB
def get_top_hashtag_pairs():
        query="""
        MATCH (h1:Hashtag)<-[:TAGS]-(:Tweet)-[:TAGS]->(h2:Hashtag)
        WHERE (ID(h1) < ID(h2)) 
        RETURN h1.Word AS source, h2.Word AS target, COUNT(*) AS weight 
        ORDER BY weight DESC 
        LIMIT 20
        """
        return graph.cypher.execute(query)


#Returns a list of hashtags that co-occur the most along with the given hashtag word in our Neo4j DB        
def hashtag_cooccurence(hashtag_word):
       #Hashtag Co-occurence
        query = """
        MATCH (h0:Hashtag)<-[:TAGS]-(:Tweet)-[:TAGS]->(h:Hashtag)
        WHERE h0.Word={hashtag_word}
        RETURN {hashtag_word} as source, h.Word AS target, COUNT(*) AS weight
        ORDER BY weight DESC
        LIMIT 20
        """
        return graph.cypher.execute(query,hashtag_word=hashtag_word)
#Example:
    # hashtag='Grammys'
    # hashtag_cooccurences = hashtag_cooccurence(hashtag)
   
#Returns a list of users that are co-followed the most along with the given user (user's screen name) in our Neo4j DB
def user_cofollowers(user_name):
        #User top cofollowers
        query=""" 
        MATCH (user)<-[:FOLLOWS]-(person)-[:FOLLOWS]->(user2)
        WHERE user.Screen_Name = {user_name}
        RETURN user2.Screen_Name as target, COUNT(*) as weight
        ORDER BY weight DESC 
        LIMIT 10
        """
        return graph.cypher.execute(query,user_name=user_name)