from boto import AWSBoto

from database import connection
import logging
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger("boto")
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler('boto.log', interval=1, backupCount=3, when='d')
formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

handler.setFormatter(formatter)
logger.addHandler(handler)



def main():
    # class Bedrock chat
    b = AWSBoto('anthropic.claude-instant-v1')
    all_posts = get_posts()
    logger.info(f"count posts - {len(all_posts)}")
    for post in all_posts:
        logger.info(post[0])
        prompt = f"""
            Describe the attitude of the auther of the post based on the post text, when -10 is pro Palistine/pro Gaza and 10 is Pro Israel, 0 is neutral. Answer number only, it very impotant! Post text: {post[1]}
        """
        # clear the chat
        b.requests = ""
        # send first message
        b.send(prompt)
        # get the last cloud messsage
        try:
            score = int(b.message.strip())
        except:
            logger.error("EXCEPTION in main", exc_info=True)
            score = 0
        # send second message with context of first
        b.send("Is this post is defamatory towards Isarel people or jews? Answer only True or False.")
        if b.message.strip() == 'True':
            defamatory = True
        elif b.message.strip() == 'False':
            defamatory = False
        else:
            defamatory = None
        logger.info(f"{score}, {defamatory}")
        update_row(post[0], score, defamatory)


def get_posts():
    with connection() as cursor:
        cursor.execute(f"""
            SELECT id, post_text FROM snpi_employee_posts
            where score is null
            order by random()
            --limit 5
        """)
        rows = cursor.fetchall()
        return rows
    
def update_row(uid, score, defamatory):
    with connection() as cursor:
        cursor.execute(f"""
            UPDATE snpi_employee_posts SET score = %s,
            defamatory = %s 
            WHERE id = %s
        """, (score, defamatory, uid))


if __name__ == '__main__':
    main()