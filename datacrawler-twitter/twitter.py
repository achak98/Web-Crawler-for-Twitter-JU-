import tweepy
import config
import json
import csv
import sys
import queue
import time
import redis
import string
import os
import argparse
from datetime import datetime
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

auth1 = OAuthHandler(config.consumer_key1, config.consumer_secret1)    #setting up authorisation handler
auth1.set_access_token(config.access_token1, config.access_secret1)    #setting up access token data
api_d = tweepy.API(auth1, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)  #passing the authentication data
auth2 = OAuthHandler(config.consumer_key2, config.consumer_secret2)    #setting up authorisation handler
auth2.set_access_token(config.access_token2, config.access_secret2)    #setting up access token data
api_rep = tweepy.API(auth2, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)  #passing the authentication data
auth3 = OAuthHandler(config.consumer_key3, config.consumer_secret3)    #setting up authorisation handler
auth3.set_access_token(config.access_token3, config.access_secret3)    #setting up access token data
api_retw = tweepy.API(auth3, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)  #passing the authentication data

auth4 = OAuthHandler(config.consumer_key4, config.consumer_secret4)    #setting up authorisation handler
auth4.set_access_token(config.access_token4, config.access_secret4)    #setting up access token data
api_follow = tweepy.API(auth4, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)  #passing the authentication data

REDIS_FOLLOWERS_KEY = "followers:%s"
REDIS_FOLLOWERS_KEY1 = "followers:%s"
#defining dictionary
#reply_data = {
 #                   "type": "FeatureCollection",
  #                  "features": []
   #                 }

reply_data = {}
reply_data['type']="FeatureCollection"
reply_data['features']=[]
# Retweeter who have not yet been connected to the social graph
unconnected = {}
# Retweeters connected to the social graph...become seeds for deeper search
connected = queue.Queue()
# Social graph
links = []
nodes = []

# Retweeter who have not yet been connected to the social graph
unconnected1 = {}
# Retweeters connected to the social graph...become seeds for deeper search
connected1 = queue.Queue()
# Social graph
links1 = []
nodes1 = []


dataDir='data'

def addUserToSocialGraph (parent, child):

    global links;


    if (child):
        nodes.append ({'id':child.id,
                       'screen_name':child.screen_name,
                       'followers_count':child.followers_count,
                       'profile_image_url':child.profile_image_url,
                       'location': child.location})

        # TODO: Find child and parent indico0 es in nodes in order to create the links
        if (parent):

            print ("Adding to socialgraph: %s ==> %s" % (parent.screen_name, child.screen_name))
            links.append ({'source':getNodeIndex (parent),
                           'target':getNodeIndex (child)})

def addUserToSocialGraph1 (parent, child):

    global links1;


    if (child):
        nodes1.append ({'id':child.id,
                       'screen_name':child.screen_name,
                       'followers_count':child.followers_count,
                       'profile_image_url':child.profile_image_url,
                       'location': child.location})

        # TODO: Find child and parent indico0 es in nodes in order to create the links
        if (parent):

            print ("Adding to socialgraph: %s ==> %s" % (parent.screen_name, child.screen_name))
            links1.append ({'source':getNodeIndex1 (parent),
                           'target':getNodeIndex1 (child)})



def getNodeIndex (user):

    global nodes
    for i in range(len(nodes)):
        if (user.id == nodes[i]["id"]):
            return i

    return -1

def getNodeIndex1 (user):

    global nodes1
    for i in range(len(nodes1)):
        if (user.id == nodes1[i]["id"]):
            return i

    return -1



def isFollower (parent, child):

    #global red

    # Fetch data from Twitter if we dont have it
    key = REDIS_FOLLOWERS_KEY % parent.screen_name
    if ( not red.exists (key) ):
        print ("No follower data for user %s" % parent.screen_name)
        crawlFollowers (parent)

    cache_count = red.hlen (key)
    if ( parent.followers_count > (cache_count*1.1) ):
        print ("Incomplete follower data for user %s. Have %d followers but should have %d (exceeds 10% margin for error)."
               % (parent.screen_name, cache_count, parent.followers_count))
        crawlFollowers (parent)

    return red.hexists (key, child.screen_name)

def isFollower1 (parent, child):

    #global red

    # Fetch data from Twitter if we dont have it
    key1 = REDIS_FOLLOWERS_KEY1 % parent.screen_name
    if ( not red1.exists (key1) ):
        print ("No follower data for user %s" % parent.screen_name)
        crawlFollowers1 (parent)

    cache_count = red1.hlen (key1)
    if ( parent.followers_count > (cache_count*1.1) ):
        print ("Incomplete follower data for user %s. Have %d followers but should have %d (exceeds 10% margin for error)."
               % (parent.screen_name, cache_count, parent.followers_count))
        crawlFollowers1 (parent)

    return red1.hexists (key1, child.screen_name)



def crawlFollowers (user):

    print ("Retrieving followers for %s (%d)" % (user.screen_name, user.followers_count))
    count = 0
    follower_cursors = tweepy.Cursor (api_follow.followers, id = user.id)
    followers_iter = follower_cursors.items()
    follower = None
    while True:
        try:
            # We may have to retry a failed follower lookup
            if ( follower is None ):
                follower = followers_iter.next()

                # Add link to Redis
                red.hset ("followers:%s" % user.screen_name, follower.screen_name, follower.followers_count)

                follower = None
                count += 1

        except StopIteration:
            break
        except tweepy.error.TweepError as err:
            print ("Caught TweepError: %s" % (err))
            if (err.reason == "Not authorized" ):
                print ("Not authorized to see users followers. Skipping.")
                break
            limit = api_follow.rate_limit_status()
            if (limit['remaining_hits'] == 0):
                seconds_until_reset = int (limit['reset_time_in_seconds'] - time.time())
                print ("API request limit reached. Sleeping for %s seconds" % seconds_until_reset)
                time.sleep (seconds_until_reset + 5)
            else:
                print ("Sleeping a few seconds and then retrying")
                time.sleep (5)

    print ("Added %d followers of user %s" % (count, user.screen_name))

    def crawlFollowers1 (user):

        print ("Retrieving followers for %s (%d)" % (user.screen_name, user.followers_count))
        count = 0
        follower_cursors = tweepy.Cursor (api_follow.followers, id = user.id)
        followers_iter = follower_cursors.items()
        follower = None
        while True:
            try:
                # We may have to retry a failed follower lookup
                if ( follower is None ):
                    follower = followers_iter.next()

                    # Add link to Redis
                    red1.hset ("followers:%s" % user.screen_name, follower.screen_name, follower.followers_count)

                    follower = None
                    count += 1

            except StopIteration:
                break
            except tweepy.error.TweepError as err:
                print ("Caught TweepError: %s" % (err))
                if (err.reason == "Not authorized" ):
                    print ("Not authorized to see users followers. Skipping.")
                    break
                limit = api_follow.rate_limit_status()
                if (limit['remaining_hits'] == 0):
                    seconds_until_reset = int (limit['reset_time_in_seconds'] - time.time())
                    print ("API request limit reached. Sleeping for %s seconds" % seconds_until_reset)
                    time.sleep (seconds_until_reset + 5)
                else:
                    print ("Sleeping a few seconds and then retrying")
                    time.sleep (5)

        print ("Added %d followers of user %s" % (count, user.screen_name))


def get_retweet (tweetId,dataDir):

    outdirectory_fn= "data/%s/retweet/follow_network/" % (query_fname)
    os.mkdir(outdirectory_fn)
    outdirectory_ch= "data/%s/retweet/retweet_chain/" % (query_fname)
    os.mkdir(outdirectory_ch)

    print (api_retw.rate_limit_status())

    # Get original Tweet details
    status = api_retw.get_status (tweetId)
    connected.put(status.user)
    addUserToSocialGraph (None, status.user)
    retweets = api_retw.retweets (status.id)
    #print ("Tweet %s, originally posted by %s, was retweeted by..." % (status.id, status.user.screen_name))
    filename = '%s_quote.json'%status.id
    f = open (os.path.join(os.getcwd(), outdirectory_ch, filename), 'a')

    tweetdetails = {'id':status.id,
                    'timestamp_source': status.created_at,
                    'retweet_count':status.retweet_count}
    f.write(json.dumps({'retweet':tweetdetails}, indent=1))
    for retweet in retweets:
        tweetdetails = {'id':retweet.id,
                        'timestamp': retweet.created_at,
                        'text':retweet.text,
                        'author':retweet.user.screen_name,
                        'location':retweet.user.location}
        f.write(json.dumps({'retweet':tweetdetails}, indent=1))

        unconnected[retweet.user.screen_name] = retweet.user;

    f.close

    # Pivot
    while not (connected.empty() or len(unconnected)==0):
        # Get next user
        pivot = connected.get()

        # Check followers of this user against unconnected retweeters
        print ("Looking through followers of %s" % pivot.screen_name)
        for (screen_name, retweeter) in unconnected.items():
            if (isFollower(pivot, retweeter)):
                print ("%s <=== %s" % (pivot.screen_name, retweeter.screen_name))
                connected.put (retweeter)
                addUserToSocialGraph (pivot, retweeter)
                del unconnected[retweeter.screen_name]
            else:
                print ("%s <=X= %s" % (pivot.screen_name, retweeter.screen_name))


    # Add unconnected nodes to social graph
    for (screen_name, user) in unconnected.items():
        addUserToSocialGraph (None, user)

    # Encode data as JSON
    currentDir=os.getcwd()
    filename = '%sfollownetwork.json'%status.id
    tweet = {'id':status.id,
             'retweet_count':status.retweet_count,
             'time_stamp':status.created_at,
             'text':status.text,
             'author':status.user.id,
             'location':status.user.location}
    with open (os.path.join(currentDir,outdirectory_fn,filename), 'a') as g:
        g.write(json.dumps({'tweet':tweet, 'nodes':nodes, 'links':links}, indent=2))



#defining parser i/ps
def get_parser():
    parser = argparse.ArgumentParser()
    #defining semantics for query input
    parser.add_argument("-q",
                        dest="query",
                        default='-')

    return parser

def get_reply_chain(tweetid,count):

    print (api_rep.rate_limit_status())

    j = api_rep.get_status(tweetid, tweet_mode='extended')

    unconnected1[j.user.screen_name] = j.user;
    # Pivot
    while not (connected1.empty() or len(unconnected1)==0):
        # Get next user
        pivot = connected1.get()

        for (screen_name, reply_er) in unconnected1.items():
            if (isFollower1(pivot, reply_er)):
                print ("%s <=== %s" % (pivot.screen_name, reply_er.screen_name))
                connected1.put (reply_er)
                addUserToSocialGraph1 (pivot, reply_er)
                del unconnected1[reply_er.screen_name]
            else:
                print ("%s <=X= %s" % (pivot.screen_name, reply_er.screen_name))


    # Add unconnected nodes to social graph
    for (screen_name, user) in unconnected1.items():
        addUserToSocialGraph1 (None, user)



    print("loading json /n/n")
    #j = json.loads (status)
    #takes in required data
    reply_json_features = {
        "type": "features",
        "%s_userid"%count: j.user.screen_name,
        "%s_location"%count: j.user.location,
        "%s_properties"%count: {
            "id":j.id_str,
            "text": j.text,
            "created_at": j.created_at,
            "parent_tweet": j.in_reply_to_status_id_str
        }
    }
    #appends the desired data into the dict structure
    print("appending")
    reply_data['features'].append(reply_json_features)
    if j.in_reply_to_status_id is not None :
        status1 = api_rep.get_status(j.in_reply_to_status_id_str)
        count=count+1
        get_reply_chain(status1.id,count)



#formats the filename of the stored json files
def format_filename(fname):

    return ''.join(convert_valid(one_char) for one_char in fname)

#checks whether each character in the proposed file name (sent one at a time)
#is either an ASCII alphanumerical or not (if not, then a default character is
#sent such that it won't be troublesome for the OS to handle
def convert_valid(one_char):

    valid_chars = "-_.%s%s" % (string.ascii_letters, string.digits)
    if one_char in valid_chars:
        return one_char
    else:
        return '_'

#main function
if __name__ == '__main__':
    global red
    parser = get_parser() #initialising parser instance
    args = parser.parse_args()  #fetching parsed arguments
    red = redis.Redis(unix_socket_path="/var/run/redis/redis-server.sock")
    red1 = redis.Redis(unix_socket_path="/var/run/redis/redis-server.sock")

    #converting filename into OS acceptable form
    query_fname = format_filename(query)
    #defining the name of the file which will contain GPS coordinates
    outdirectory= "data/%s/reply/ch/" % (query_fname)
    os.mkdir(outdirectory)
    outfile = "replychain_%s.json" % (query_fname)

    for tweet in tweepy.Cursor(api_d.search, q=args.query, lang="en").items(5000000):

    try:

        count=0
        with open(os.path.join(os.getcwd(),outdirectory,outfile), 'a') as h:
            print("opening file")
            #converting str to dict format
            j=json.loads(data)
            get_retweet(j['id_str'],dataDir)
            #checking whether the desired data field contains valid data
            if j['in_reply_to_status_id']:
                connected1.put(j.user)
                addUserToSocialGraph1 (None, j.user)
                print("going to recursive /n/n")
                get_reply_chain(j['id_str'],count)
                #writes/dumps the desired data into json file
                print("writing")
                h.write(json.dumps(reply_data, indent=4, sort_keys=True, default=str))
                print("write done")
                # Encode data as JSON
                currentDir=os.getcwd()
                outdirectory_fn="data/%s/reply/fn/" % (query_fname)
                os.mkdir(outdirectory_fn)
                filename = '%sfollownetwork.json'%j.id
                reply = {'id':j.id,
                         'retweet_count':j.retweet_count,
                         'time_stamp':j.created_at,
                         'text':j.text,
                         'author':j.user.id,
                         'location':j.user.location}
                with open (os.path.join(currentDir,outdirectory_fn,filename), 'a') as g:
                    g.write(json.dumps({'end_node':reply, 'nodes':nodes1, 'links':links1}, indent=2))

        return True

    #exception handler
    except BaseException as e:
        print("Error on_data: %s" % str(e))
        time.sleep(5)
        return True
