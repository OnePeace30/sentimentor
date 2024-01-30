from boto import AWSBoto

from database import connection
import logging
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger("boto")
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler('boto2.log', interval=1, backupCount=3, when='d')
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
            Please tell me where {post[0]}
                    can fit from the list i give you.

            you can only chose from this list:
            Science and Technology
            Business and Economics
            Health and Medicine
            Social Sciences, Humanities, and Law
            Interdisciplinary and Applied Studies

            straight forward anwser replay only with the names from the list i gave you, nothing more
        """
        # clear the chat
        b.requests = ""
        # send first message
        b.send(prompt)
        # get the last cloud messsage
        try:
            logger.info(b.message)
            score = b.message.strip()
            update_row(post[0], score)
        except:
            logger.error("EXCEPTION in main", exc_info=True)
            score = 0
        # send second message with context of first
        # b.send("Is this post is defamatory towards Isarel people or jews? Answer only True or False.")
        # if b.message.strip() == 'True':
        #     defamatory = True
        # elif b.message.strip() == 'False':
        #     defamatory = False
        # else:
        #     defamatory = None
        # logger.info(f"{score}, {defamatory}")
        


def get_posts():
    with connection() as cursor:
        cursor.execute(f"""
            SELECT field_of_study FROM snpi_grouped_field_of_study
            where field_of_study_group is null
            order by random()
            --limit 5
        """)
        rows = cursor.fetchall()
        return rows
    
def update_row(uid, score):
    with connection() as cursor:
        cursor.execute(f"""
            UPDATE snpi_grouped_field_of_study SET field_of_study_group = %s
            WHERE field_of_study = %s
        """, (score, uid))




if __name__ == '__main__':
    main()
    