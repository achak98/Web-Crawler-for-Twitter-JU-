import tweepy
import csv
import config

# Twitter API credentials
consumer_key = config.consumer_key
consumer_secret = config.consumer_secret
access_key = config.access_key
access_secret = config.access_secret


def get_all_tweets(screen_name):
    # Twitter only allows access to a users most recent 3240 tweets with this method

    # authorize twitter, initialize tweepy
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    # initialize a list to hold all the tweepy Tweets
    alltweets = []

    # make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.user_timeline(screen_name=screen_name, count=200)

    # save most recent tweets
    alltweets.extend(new_tweets)

    # save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1

    # keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print
        "getting tweets before %s" % (oldest)

        # all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(screen_name=screen_name, count=200, max_id=oldest)

        # save most recent tweets
        alltweets.extend(new_tweets)

        # update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1

        print
        "...%s tweets downloaded so far" % (len(alltweets))

    # transform the tweepy tweets into a 2D array that will populate the csv
    outtweets = [[tweet.id_str, tweet.created_at, tweet.text.encode("utf-8"), tweet.favorite_count, tweet.retweet_count]
                 for tweet in alltweets]

    # write the csv
    with open('%s_tweets.csv' % screen_name, mode='w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["id", "created_at", "text"])
        writer.writerows(outtweets)

    pass

def main():
    get_all_tweets("tartecosmetics")
    user_name = "@nameofuser"

    replies = tweepy.Cursor(api.search, q='to:{}'.format(user_name),
                                    since_id=tweet_id, tweet_mode='extended').items()
    while True:
        try:
            reply = replies.next()
            if not hasattr(reply, 'in_reply_to_status_id_str'):
                continue
            if reply.in_reply_to_status_id == tweet_id:
               logging.info("reply of tweet:{}".format(reply.full_text))

        except tweepy.RateLimitError as e:
            logging.error("Twitter api rate limit reached".format(e))
            time.sleep(60)
            continue

        except tweepy.TweepError as e:
            logging.error("Tweepy error occured:{}".format(e))
            break

        except StopIteration:
            break

        except Exception as e:
            logger.error("Failed while fetching replies {}".format(e))
            break


if __name__ == '__main__':
    main()
