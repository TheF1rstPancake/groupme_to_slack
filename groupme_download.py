import groupy
import argparse
import sys
import sqlite3
import os
import errno

def createUserTable(conn):
    c = conn.cursor()

    c.execute(
        "CREATE TABLE IF NOT EXISTS users("
        "id INTEGER UNIQUE PRIMARY KEY, "
        "name TEXT NOT NULL, "
        "image_url TEXT)"
    )
    conn.commit()

def createMessagesTable(conn):
    """
    Create `messages` table with the following schema:
        - id (PRIMARY KEY): the id of the message
        - user_id (INT):    id of the user who sent the message
        - text:             the message text
        - date (NUMERIC):   UNIX timestamp for when the message was created
    """
    c = conn.cursor()

    c.execute(
        "CREATE TABLE IF NOT EXISTS messages("
        "id INTEGER UNIQUE PRIMARY KEY, "
        "user_id INTEGER NOT NULL, "
        "text TEXT, "
        "date NUMERIC)"
    )
    conn.commit()

def createAttachmentTable(conn):
    """
    Create a table to hold attachments with the following schema:
        message_id (INT):   the id of the message
                            that the attachment belongs to
        location (TEXT):    the location of where the file was downloaded
        type (TEXT):        a string indicating the type of attachment.
        content(TEXT):      the content of the attachment

    .. note::
        This currently only supports `image`
    """
    c = conn.cursor()

    c.execute(
        "CREATE TABLE IF NOT EXISTS attachments("
        "message_id INTEGER NOT NULL, "
        "type TEXT NOT NULL, "
        "content TEXT NOT NULL, "
        "location TEXT)"
    )
    conn.commit()

def addUser(conn, user):
    """
    Add a user to the database.  Takes in a Groupy member object
    """
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO users VALUES(?, ?, ?)",
        (user.user_id, user.nickname, user.image_url)
    )

def addUsers(conn, users):
    """
    add a list of users to the database.
    Takes in a list of Groupy member objects
    """
    for u in users:
        addUser(conn, u)
    conn.commit()


def addAttachment(
    conn, message_id, attachment,
    attachment_type="image", location=None
):
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO attachments VALUES (?, ?, ?, ?)",
        (message_id, attachment_type, attachment.url, location)
    )


def addMessages(
    conn,
    messages,
    download_attachment=False,
    download_location="attachments/"
):
    c = conn.cursor()
    for m in messages:
        c.execute(
            "INSERT OR REPLACE INTO messages VALUES(?, ?, ?, ?)",
            (m.id, m.user_id, m.text, m.created_at.timestamp())
        )
        # check if there are any attachments and if so handle them
        if m.attachments:
            for a in m.attachments:
                if a.type == "image":
                    location = None
                    if a.type == "image" and download_attachment:
                        location = a.url.split(".")[-1]
                        location = os.path.join(download_location, location)
                        p = a.download()
                        p.save(location, format=p.format)

                    # add the attachment to the database
                    addAttachment(
                        conn, m.id, a,
                        attachment_type="image", location=location
                    )
    conn.commit()


def getMessages(conn, group, members=None, download_attachment=False, download_location="attachments/"):
    if members is None:
        members = group.members()

    # create our message and attachment tables
    createMessagesTable(conn)
    createAttachmentTable(conn)

    # if download_attachment is true,
    # make sure our final location is initialized
    try:
        path = download_location
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    # we are going to get all messages, so we will keep looping until
    # the API doesn't return anything new
    print("Getting a batch of messages")
    messages = group.messages()
    message_count = 0
    while messages:
        message_count = message_count + len(messages)
        print(
            "Received: {0} messages ({1:.2f})".format(
                len(messages), message_count/group.message_count
            )
        )
        # add the messages to the database
        addMessages(conn, messages, download_attachment, download_location)

        # get the next batch of messages
        messages = messages.older()


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        "group",
        help="Name of the group whose messages you want to download",
    )
    arg_parser.add_argument(
        "--database",
        help="Name of the file where you want the SQLite database to be saved",
        default="database.db"
    )
    arg_parser.add_argument(
        "--download_attachment",
        help="Set this flag if you want images to be downloaded locally",
        action="store_true"
    )
    arg_parser.add_argument(
        "--download_location",
        help="Directory where attachments should be saved",
        default="attachments"
    )
    args = arg_parser.parse_args()

    print("Looking for group: {0}".format(args.group))
    group_list = groupy.Group.list()
    group = group_list.first \
        if args.group is None \
        else [i for i in group_list if i.name == args.group][0]

    if not group:
        print("No group found!  Please try again")
        sys.exit(1)

    print("Getting users!")
    members = group.members()

    # add users to the databases
    conn = sqlite3.connect(args.database)

    print("Writing users to database")
    createUserTable(conn)
    addUsers(conn, members)

    print("Getting messages")
    getMessages(
        conn, group, members, args.download_attachment, args.download_location
    )

    #close connection
    conn.close()
