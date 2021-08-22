import tweepy
import time
import argparse
import string
import string
import config
import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

#defining dictionary
reply_data = {
                    "type": "FeatureCollection",
                    "features": []
                    }

#defining parser i/ps
def get_parser():
    parser = argparse.ArgumentParser()
    #defining semantics for query input
    parser.add_argument("-q",
                        dest="query",
                        default='-')
    #defining semantics for data destination directory input
    parser.add_argument("-d",
                        dest="dataDir",
                        default="data")
    return parser

def get_reply_chain(tweetid,count):
    j = api.get_status(tweetid)
    print("loading json /n/n")
    #j = json.loads (status)
    #takes in required data
    reply_json_features = {
        "type": "Feature",
        "%s_userid"%count: j.user.screen_name,
        "%s_location"%count: j.user.location,
        "%s_properties"%count: {
            "text": j.text,
            "created_at": j.created_at,
            "parent_tweet": j.in_reply_to_status_id_str
        }
    }
    #appends the desired data into the dict structure
    print("appending")
    reply_data['features'].append(reply_json_features)
    if j.in_reply_to_status_id is not None :
        status1 = api.get_status(j.in_reply_to_status_id_str)
        count=count+1
        get_reply_chain(status1.id,count)

class MyListener(StreamListener):

    #defining the constructor
    def __init__(self, dataDir, query):
        #converting filename into OS acceptable form
        query_fname = format_filename(query)
        #defining the name of the file which will contain GPS coordinates
        self.outfile = "%s/stream_%s.json" % (dataDir, query_fname)


    #defining the on_data method which decides how the data in the stream is processed
    def on_data(self, data):

        try:
            reply_data.clear
            count=0
            with open(self.outfile, 'a') as f:
                print("opening file")
                #converting str to dict format
                j=json.loads(data)
                #checking whether the desired data field contains valid data
                if j['in_reply_to_status_id']:
                    print("going to recursive /n/n")
                    get_reply_chain(j['id_str'],count)
                    #writes/dumps the desired data into json file
                    print("writing")
                    f.write(json.dumps(reply_data, indent=4, sort_keys=True, default=str))
                    print("write done")

            return True

        #exception handler
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            time.sleep(5)
            return True

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
    parser = get_parser() #initialising parser instance
    args = parser.parse_args()  #fetching parsed arguments
    auth = OAuthHandler(config.consumer_key1, config.consumer_secret1)    #setting up authorisation handler
    auth.set_access_token(config.access_token1, config.access_secret1)    #setting up access token data
    api = tweepy.API(auth)  #passing the authentication data

    twitter_stream = Stream(auth, MyListener(args.dataDir, args.query)) #describing the streamer
    twitter_stream.filter(track=[args.query])   #initiating the streamer with the given query
