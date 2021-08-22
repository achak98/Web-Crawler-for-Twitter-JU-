import tweepy
import config
import time
import string
import os
import sys
import json
import argparse
from datetime import datetime
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

FOLLOWING_DIR = 'following'
USER_DIR = 'twitter-users'
MAX_FRIENDS = 200
FRIENDS_OF_FRIENDS_LIMIT = 200

# Create the directories we need
if not os.path.exists(FOLLOWING_DIR):
    os.makedirs(FOLLOWING_DIR)

if not os.path.exists(USER_DIR):
    os.makedirs(USER_DIR)

#defining parser i/ps
def get_parser():
    parser = argparse.ArgumentParser()
    #defining semantics for query input
    parser.add_argument("-sn",
                        dest="screen_name",
                        default='-')
    #defining semantics for data destination directory input
    parser.add_argument("-d",
                        dest="depth",
                        default="1")
    return parser


def get_follower_ids(centre, max_depth=1, current_depth=0, taboo_list=[]):


    if current_depth == max_depth:
        print ('out of depth')
        return taboo_list

    if centre in taboo_list:
        # we've been here before
        print ('Already been here.')
        return taboo_list
    else:
        taboo_list.append(centre)

    try:
        fname = os.path.join(USER_DIR, format_filename(str(centre)) + '.json')
        if not os.path.exists(fname):
            print ('Retrieving user details for twitter id %s' % str(centre))
            while True:
                try:
                    user = api.get_user(centre)

                    d = {'name': user.name,
                         'screen_name': user.screen_name,
                         'profile_image_url' : user.profile_image_url,
                         'created_at' : str(user.created_at),
                         'id': user.id,
                         'friends_count': user.friends_count,
                         'followers_count': user.followers_count,
                         'followers_ids': user.followers_ids()}

                    with open(fname, 'w') as outf:
                        outf.write(json.dumps(d, indent=1))

                    user = d
                    break
                except tweepy.TweepError as error:
                    print (type(error))

                    if str(error) == 'Not authorized.':
                        print ('Can''t access user data - not authorized.')
                        return taboo_list

                    if str(error) == 'User has been suspended.':
                        print ('User suspended.')
                        return taboo_list

                    errorObj = error[0][0]

                    print (errorObj)

                    if errorObj['message'] == 'Rate limit exceeded':
                        print ('Rate limited. Sleeping for 15 minutes.')
                        time.sleep(15 * 60 + 15)
                        continue

                    return taboo_list
        else:
            user = json.loads(file(fname).read())

        screen_name = enc(user['screen_name'])
        fname = os.path.join(FOLLOWING_DIR, format_filename(screen_name) + '.csv')
        friendids = []

        if not os.path.exists(fname):
            print ('No cached data for screen name "%s"' % screen_name)
            with open(fname, 'w') as outf:
                params = (enc(user['name']), screen_name)
                print ('Retrieving friends for user "%s" (%s)' % params)

                # page over friends
                c = tweepy.Cursor(api.friends, id=user['id']).items()

                friend_count = 0
                while True:
                    try:
                        friend = c.next()
                        friendids.append(friend.id)
                        params = (friend.id, enc(friend.screen_name), enc(friend.name))
                        outf.write('%s\t%s\t%s\n' % params)
                        friend_count += 1
                        if friend_count >= MAX_FRIENDS:
                            print ('Reached max no. of friends for "%s".' % friend.screen_name)
                            break
                    except tweepy.TweepError:
                        # hit rate limit, sleep for 15 minutes
                        print ('Rate limited. Sleeping for 15 minutes.')
                        time.sleep(15 * 60 + 15)
                        continue
                    except StopIteration:
                        break
        else:
            friendids = [int(line.strip().split('\t')[0]) for line in file(fname)]

        print ('Found %d friends for %s' % (len(friendids), screen_name))

        # get friends of friends
        cd = current_depth
        if cd+1 < max_depth:
            for fid in friendids[:FRIENDS_OF_FRIENDS_LIMIT]:
                taboo_list = get_follower_ids(fid, max_depth=max_depth,
                    current_depth=cd+1, taboo_list=taboo_list)

        if cd+1 < max_depth and len(friendids) > FRIENDS_OF_FRIENDS_LIMIT:
            print ('Not all friends retrieved for %s.' % screen_name)

    except Exception as error:
        print ('Error retrieving followers for user id: ', centre)
        print (error)

        if os.path.exists(fname):
            os.remove(fname)
            print ('Removed file "%s".' % fname)

        sys.exit(1)

    return taboo_list

    
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

    depth = int(args.depth)
    screen_name = args.screen_name

    #converting filename into OS acceptable form
    screen_fname = format_filename(screen_name)
    #defining the name of the file which will contain GPS coordinates
    fname = os.path.join(FOLLOWING_DIR, screen_fname + '.csv')

 

    if depth < 1 or depth > 5:
        print ('Depth value %d is not valid. Valid range is 1-5.' % depth)
        sys.exit('Invalid depth argument.')

    print ('Max Depth: %d' % depth)
    matches = api.lookup_users(screen_names=[screen_name])

    if len(matches) == 1:
        print (get_follower_ids(matches[0].id, max_depth=depth))
    else:
        print ('Sorry, could not find twitter user with screen name: %s' % twitter_screen_name)
    
 

                    

