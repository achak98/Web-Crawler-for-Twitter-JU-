import tweepy
from tweepy import OAuthHandler
import config
import json
import csv
import sys

consumer_key = config.consumer_key
consumer_secret = config.consumer_secret
access_token = config.access_token
access_secret = config.access_secret

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth,  wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

myFields = ['User', 'Reply-er', 'text']

def get_parser():
    parser = argparse.ArgumentParser()
    #defining semantics for query input
    parser.add_argument("-u",
                        dest="user")
    return parser

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


parser = get_parser() #initialising parser instance
args = parser.parse_args()  #fetching parsed arguments

fname=format_filename(args.user)

replies=[]
non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)

myFile = open('replies_%s.csv'%fname, 'w')
with myFile:

    writer = csv.DictWriter(myFile, fieldnames=myFields)
    writer.writeheader()
    for full_tweets in tweepy.Cursor(api.user_timeline,screen_name=user,timeout=999999).items(10):
      for tweet in tweepy.Cursor(api.search,q='to:%s'%user, since_id=992433028155654144, result_type='recent',timeout=999999).items(1000):
        if hasattr(tweet, 'in_reply_to_status_id_str'):
          if (tweet.in_reply_to_status_id_str==full_tweets.id_str):
            replies.append(tweet.text)
            writer.writerow({'User' : user, 'Reply-er' : tweet.user.screen_name, 'text' : tweet.text})
      print("Tweet :",full_tweets.text.translate(non_bmp_map))
      for elements in replies:
           print("Replies :",elements)
      replies.clear()
