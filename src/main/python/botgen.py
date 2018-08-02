import sys
import json
import random
from datetime import datetime, timedelta
import time

#########################
# Ad logs json generator
#
# Help : python botgen.py -h
#
# Examples :
# Write log for 1 bot, 1000 users, 100 requestes/sec, duration 300 seconds
#
#   python botgen.py -b 1 -u 1000 -n 100 -d 300 -f data.json
#
# Notes :
#  bots have ip 172.20.X.X and make a transition  ~ 1 in sec
#  users have ip 172.10.X.X and make a transition  ~ 4 in sec
#

# == generate content ids for bots and users
# content ids [1000 .. 1020]
bot_categories = [id for id in range(1000, 1020)]
# bot changes content twice as much as an user
# conten ids [1000, 1000 ..  1010, 1010]
user_categories = bot_categories[:int(len(bot_categories)/2)]*2

# these funtions return random content id for users, bots
def random_content_user(): return random.choice(user_categories)
def random_content_bot(): return random.choice(bot_categories)

# generate random action for users, bots
# bots clicks more often that users
def random_action_user(): return random.choice(['click', 'view', 'view', 'view'])   # probabilities click/view = 25/75
def random_action_bot(): return random.choice(['click', 'click', 'click', 'view'])  # probabilities click/view = 75/25

def user2ip(id): return "172.10.{}.{}".format(int(id / 255), id % 255)
def bot2ip(id): return "172.20.{}.{}".format(int(id / 255), id % 255)
def asits(dt): return int(dt.timestamp())

def asJson(entry): return { 'unix_time' : asits(entry[0]), 'category_id': entry[1], 'ip' : entry[2], 'type' : entry[3] }

# Log generator for users & bots
def generate_log(args, start_time):

    t1, t2 = start_time, start_time + timedelta(seconds = args.duration)

    users = range(0, args.users)
    while t1 < t2:
        for uid in random.sample(users, args.freq):
            yield (t1, random_content_user(), user2ip(uid), random_action_user())

        for bid in range(0, args.bots):
            for uid in random.sample(users, args.bot_freq):
                yield (t1, random_content_bot(), bot2ip(bid), random_action_bot())

        t1 += timedelta(seconds = 1)
        time.sleep(1)

    print("generated for period :", start_time,  t2)


def do_generate(fd = None):
    for entry in generate_log(args, datetime.now()):
        jsonString = json.dumps(asJson(entry))
        if fd:
            fd.write(jsonString + "\n")
        else:
            print(jsonString)

def main(args):
    print("started with parameters :", args)
    if args.file:
        with open(args.file, 'w') as fd:
            do_generate(fd)
    else:
        do_generate()

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',  '--duration', type=int, default=5,     help="script running time in seconds")
    parser.add_argument('-b',  '--bots',     type=int, default=1,     help="number of bots")
    parser.add_argument('-u',  '--users',    type=int, default=60000, help="number of users")
    parser.add_argument('-ru', '--freq',     type=int, default=1000,  help="number of requests for all users per second")
    parser.add_argument('-ub', '--bot_freq', type=int, default=10,    help="number of requests per bot per second")
    parser.add_argument('-f',  '--file',     type=str, default=None,  help="events file")
    args = parser.parse_args()

    main(args)
