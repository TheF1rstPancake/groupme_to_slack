from slackclient import SlackClient
import argparse
import sqlite3
import os
import json
import time

def createAttachment(message):
    return {
        "fallback": "GroupMe Message",
        "title": "GroupMe Message",
        "author_name": message['name'],
        "author_icon": message['image_url'],
        "text": message['text'] if message['text'] is not None else '',
        "image_url": message['content']
    }


def createMessage(message):
    return {
        "text": message['text'],
        "username": message['name'],
        "icon_url": message['image_url']
    }

def _addMessage(d, sc, channel_name):
    message = createMessage(d)
    attachment = createAttachment(d) if d['content'] is not None else None

    if attachment:
        message['attachments'] = [attachment]

    response = sc.api_call(
        "chat.postMessage", channel=channel_name, **message)
    if response.get("error"):
        print("ERROR on message {0}: {1}".format(d['id'], response))

def addMessages(conn, channel_name, start_index=0):
    # initialize slack api handler
    slack_token = os.environ["SLACK_API_TOKEN"]
    sc = SlackClient(slack_token)

    # create the channel
    sc.api_call("channels.create", name=args.channel_name)

    # get data out of sqlite database
    q = """SELECT * FROM messages LEFT JOIN attachments ON messages.id = attachments.message_id LEFT JOIN users ON users.id = messages.user_id ORDER BY date ASC"""
    c = conn.cursor()
    l = c.execute(q)
    data = l.fetchall()
    if start_index != 0:
        data = data[start_index:]

    num_messages = len(data)

    # format the message for slack and send
    print("Sending data to Slack")
    counter = 0
    for d in data:
        try:
            response = _addMessage(d, sc, channel_name)
        except json.decoder.JSONDecodeError as e:
            print("ERROR handling Slack response.  Likely an API rate limit")
            print("Sleeping for 5 minutes")
            time.sleep(300)
            response = _addMessage(d, sc, channel_name)

        counter = counter + 1
        if counter % 100 == 0:
            print("Processed {0} messages ({1:.2f})".format(
                counter, counter / num_messages)
            )
            print("Sleeping for 30 seconds to avoid rate limit")
            time.sleep(30)
            print("Waking up!")

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "channel_name",
        help="Name of the Slack channel where messages should go."
        "Must include '#'",
    )
    arg_parser.add_argument(
        "--database",
        help="Name of the file where you want the SQLite database to be saved",
        default="database.db"
    )

    arg_parser.add_argument(
        "--attachment_location",
        help="Directory where attachments are saved",
        default="attachments"
    )
    arg_parser.add_argument(
        "--start_index",
        help="Index of the message where you want to start uploading. This should be used in the event that the script dies and you want to pick up",
        type=int,
        default=0
    )

    args = arg_parser.parse_args()

    # connect to sqlite
    conn = sqlite3.connect(args.database)
    conn.row_factory = sqlite3.Row

    # start adding messages
    addMessages(conn, args.channel_name, start_index=args.start_index)
