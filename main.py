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
    b = AWSBoto('anthropic.claude-v2')
    all_posts = get_posts()
    logger.info(f"count posts - {len(all_posts)}")
    for post in all_posts:
        logger.info(post[0])
        prompt = f"""
            Please review {post[1]}  Approach and Strategy in Israel 
            for answers, use publicly available resources on the Internet
        """
        # clear the chat
        b.requests = ""
        # send first message
        b.send(prompt)
        # get the last cloud messsage
        try:
            logger.info(b.message)
            reaction_to_attacks = b.message
        except:
            logger.error("EXCEPTION in main", exc_info=True)
            reaction_to_attacks = None
        # send second message with context of first
        b.send(f"""Please review Recent News on {post[1]} Activities in Israel. for answers, use publicly available resources on the Internet""")
        try:
            logger.info(b.message)
            donations_due_to_the_attacks = b.message
        except:
            logger.error("exception", exc_info=True)
            donations_due_to_the_attacks = None
        b.send(f"""Please review {post[1]} Attitude Towards Conflicts and Global Challenges, in particullary on Israel-Hamas conflict. for answers, use publicly available resources on the Internet""")
        try:
            logger.info(b.message)
            plans_to_expand = b.message
        except:
            logger.error("exception", exc_info=True)
            plans_to_expand = None
        # logger.info(f"{score}, {defamatory}")
        update_row(post[0], reaction_to_attacks, donations_due_to_the_attacks, plans_to_expand)


def get_posts():
    with connection() as cursor:
        cursor.execute(f"""
            SELECT id, company_name FROM snpi_about_company
            where reaction_to_attacks is null
            order by random()
            --limit 5
        """)
        rows = cursor.fetchall()
        return rows
    
def update_row(uid, reaction_to_attacks, donations_due_to_the_attacks, plans_to_expand):
    with connection() as cursor:
        cursor.execute(f"""
            UPDATE snpi_about_company SET reaction_to_attacks = %s,
            donations_due_to_the_attacks = %s,
            plans_to_expand = %s
            WHERE id = %s
        """, (reaction_to_attacks, donations_due_to_the_attacks, plans_to_expand, uid))




if __name__ == '__main__':
    main()
    