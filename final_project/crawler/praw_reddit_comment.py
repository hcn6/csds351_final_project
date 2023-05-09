import praw
from final_project.database import db
from final_project.utils import logging
import pprint


def preprocess_comment_dict(my_dict):
    my_dict = {k: v for k, v in my_dict.items() if not k.startswith('_')}
    try:
        my_dict['author'] = my_dict['author'].name
        my_dict['subreddit'] = my_dict['subreddit'].display_name
    except Exception as e:
        return None
    return my_dict


def crawl_comment_one_post(post_id: str, total_comment: int):
    reddit = praw.Reddit('bot1')
    submission = reddit.submission(id=post_id)
    submission.comments.replace_more(limit=None)
    all_comments = submission.comments.list()
    count = 0
    for comment in all_comments:
        comment_dict = vars(comment)
        comment_dict = preprocess_comment_dict(comment_dict)
        if comment_dict is None:
            continue
        # pprint.pprint(comment_dict)
        db.get_collection('reddit_comment_praw').insert_one(comment_dict)

        count += 1
    logging.info(f"Total comments crawled: {count} Total comments in post: {total_comment}")


if __name__ == '__main__':
    count = 8688
    collection = db.get_collection('reddit_post')

    all_posts = list(collection.find({"num_comments": {"$gt":0}}))[count:]

    logging.info(collection.count_documents({}))
    logging.info('Start crawling comment')

    for post in all_posts:
        count += 1
        if post['author'] == '[deleted]':
            continue
        logging.info(f"{count}. Post ID: {post['id']} Total comments: {post['num_comments']} reddit_url: {post['url']}'")
        crawl_comment_one_post(post['id'], post['num_comments'])
        logging.info(f"{count}. Finish crawling comment for post {count}. Title: {post['title']}")
