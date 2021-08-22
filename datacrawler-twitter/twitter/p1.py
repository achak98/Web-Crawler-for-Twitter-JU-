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
geo_data = {
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

class MyListener(StreamListener):

    #defining the constructor
    def __init__(self, dataDir, query):
        #converting filename into OS acceptable form
        query_fname = format_filename(query)
        #defining the name of the file which will contain GPS coordinates
        self.outfile1 = "%s/streamNET_%s.json" % (dataDir, query_fname)
        #defining the name of the file which will contain the entire structure of the tweet
        self.outfile2 = "%s/streamGROSS_%s.json" % (dataDir, query_fname)
        #defining the name of the file which will contain the location name marker of the tweet
        self.outfile3 = "%s/streamLOC_%s.json" % (dataDir, query_fname)

    #defining the on_data method which decides how the data in the stream is processed
    def on_data(self, data):

        try:
            with open(self.outfile2, 'a') as f:
                #converting str to dict format
                j=json.loads(data)
                print(j)
                print('\n\n\n\n')
                #storing (appending) as str in json format
                f.write(json.dumps(j))
                with open(self.outfile2, 'a') as g:
                    #checking whether the desired data field contains valid data
                    if j['coordinates']:
                        #takes in required data
                        geo_json_feature = {
                            "type": "Feature",
                            "coordinates": j['coordinates'],
                            "properties": {
                                "text": j['text'],
                                "created_at": j['created_at']
                            }
                        }
                        #appends the desired data into the dict structure
                        geo_data['features'].append(geo_json_feature)
                        #writes/dumps the desired data into json file
                        g.write(json.dumps(geo_data))
                with open(self.outfile3, 'a') as h:
                    #checks if the desired field contains valid data
                    if j['user']['location']:
                        #takes in desired fields
                        geo_json_feature = {
                            "type": "Feature",
                            "location": j['user']['location'],
                            "properties": {
                                "text": j['text'],
                                "created_at": j['created_at']
                            }
                        }
                        #appends data into dict structure
                        geo_data['features'].append(geo_json_feature)
                        #dumps data into json
                        h.write(json.dumps(geo_data))
                #delay effect for the printed output to be visible
                time.sleep(1)
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
    auth = OAuthHandler(config.consumer_key, config.consumer_secret)    #setting up authorisation handler
    auth.set_access_token(config.access_token, config.access_secret)    #setting up access token data
    api = tweepy.API(auth)  #passing the authentication data
    
    twitter_stream = Stream(auth, MyListener(args.dataDir, args.query)) #describing the streamer
    twitter_stream.filter(track=[args.query])   #initiating the streamer with the given query

                    
