from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

import logging

from multiprocessing import Pool

# from progess.bar  import IncrementalBar
import csv
from tqdm import tqdm
import time

import pdb


FORMAT = '%(asctime)-15s %(message)s'
logname = input('Enter log filename')
logging.basicConfig(filename=logname, filemode='a',level=logging.INFO, format= FORMAT)
logger = logging.getLogger(__name__)

db_string = "postgres://postgres:postgres@127.0.0.1:5432/swhgd"
db = create_engine(db_string, pool_size = 8)
connection = db.connect()
logger.info("Database connected successfully")

class FindingForks:
    
    def __init__(self):
        self.collection_of_forks_details = []
        # self.db_string = "postgres://postgres:postgres@127.0.0.1:5432/swhgd"
        # self.db = create_engine(self.db_string)
        # self.connection = self.db.connect()
        # logger.info("Database connected successfully")


    def get_fork_with_authors(self,url):

        origin_id_row = connection.execute(text("select id from origin where url = :target_url "), target_url = url)
        logger.info('Step 1 complete')
        origin_id = 0
        for id in origin_id_row:
            origin_id = id['id']
            break
        # Extracting revisions of a origin
        # origin_interval_revision_rows = connection.execute(text("select distinct target from snapshot_branch where object_id in (select distinct branch_id from snapshot_branches where snapshot_id in (select distinct snapshot_id from origin_visit where origin= :origin and status='full'))"), origin = origin_id)
        origin_interval_revision_rows = connection.execute(text("select distinct snapshot_branch.target as target from origin_visit, snapshot_branches, snapshot_branch where snapshot_branch.object_id = snapshot_branches.branch_id and snapshot_branches.snapshot_id = origin_visit.snapshot_id and origin = :origin and status = 'full'"), origin = origin_id)
        logger.info('Step 2 complete')
        origin_interval_revisions = [rr['target'] for rr in origin_interval_revision_rows]
        # print(origin_revisions)
        logger.info('{} Interval Revisions of a origin: {} retrieved successfully'.format(len(origin_interval_revisions), url))

        
        # logger.info('{} Revisions of a origin: {} retrieved successfully'.format(len(origin_interval_revisions), url))

        # Retrieving child commit of parent revisions of a origin
        child_revisions = []
        child_revisions_query = connection.execute(text("select distinct id from revision_history where parent_id =  ANY(:parents)"), parents = origin_interval_revisions)
        logger.info('Step 3 complete')
        for crq in child_revisions_query:
            child_revisions.append(crq['id'])

        logger.info('{} Child revisions retrieved of origin {}'.format(len(child_revisions), url))


        # Extracting fork url, its base commit in a single query
        revisions = origin_interval_revisions + child_revisions
        logger.info('Revisions used for Step 4 is: {}'.format(len(revisions)))
        url_rows = connection.execute(text("select url, snapshot_branch.target as rev from origin, origin_visit, snapshot_branches, snapshot_branch where origin.id = origin_visit.origin and origin_visit.snapshot_id = snapshot_branches.snapshot_id and snapshot_branches.branch_id = snapshot_branch.object_id and snapshot_branch.target  = ANY(:target)  and url != :origin "), target= revisions, origin=url) # replace child revision with origin_revision_with_child_revision
        logger.info('Step 4 complete')
        # url_rows = connection.execute(text("select url, snapshot_branch.target as rev from origin, origin_visit, snapshot_branches, snapshot_branch where origin.id = origin_visit.origin and origin_visit.snapshot_id = snapshot_branches.snapshot_id and snapshot_branches.branch_id = snapshot_branch.object_id and snapshot_branch.target = ANY(:target)  and url != :origin"), target=child_revisions, origin=url)  # origin_commit = origin_interval_revisions and url like 'https://gitlab.com/%'
        # logger.info(url_rows)
        fork_urls_with_revisions = {}
        revisions_behind_finding_fork_url = {}
        
        if url_rows is not None:
            for row in url_rows:
                # logger.info("Url: {}, Revision: {}".format(row['url'], row['rev']))
                if row['url'] in fork_urls_with_revisions:
                    fork_urls_with_revisions[row['url']].add(row['rev'])
                    revisions_behind_finding_fork_url[row['url']].append(row['rev'].hex())
                else:
                    fork_urls_with_revisions[row['url']] = set()
                    revisions_behind_finding_fork_url[row['url']] = []
                # logger.info("Url: {}, Number of commits {}".format( row['url'], len(fork_urls_with_revisions[row['url']])))
        
        logger.info('Total forked url: {} of origin {}'.format(len(fork_urls_with_revisions), url))


        # for key in fork_urls_with_revisions:
        #     # logger.info("Url : {} in progress and revisions {}".format(key, fork_urls_with_revisions[key]))
        #     for i in list(fork_urls_with_revisions[key]):
        #         parent = i
        #         while 1:
        #             child_revision_row = connection.execute(text("select id from revision_history where parent_id = :parent"), parent = parent)
        #             if child_revision_row is None:
        #                 break
        #             for row in child_revision_row:
        #                 fork_urls_with_revisions[key].add(row['id'])
        #                 parent = row['id']
        #                 # logger.info("revision added and size is {}".format(len(fork_urls_with_revisions[key])))
        #             break
        #     # logger.info('{} commit of url : {} retrieved'.format(len(fork_urls_with_revisions[key]), key))

        # logger.info('{} Url retrieve successfully'.format(len(fork_urls_with_revisions)))
    
                        
        # logger.info('Retrieved child revisions of all individual fork url of origin {}'.format(url))
    # calculating revision per author count of fork urls

        for key in fork_urls_with_revisions:
            # logger.info('Url : {} started'.format(key))
            # commit_author_row = connection.execute(text("select count(id) as cnt, author from revision where id = ANY(:commits) group by author having author != ANY(:author_origin)"), commits = list(fork_urls_with_revisions[key]), author_origin = authors_origin_revisions)
            commit_author_row = connection.execute(text("select count(id) as cnt, author from revision where id = ANY(:commits) group by author"), commits = list(fork_urls_with_revisions[key]))

            for row in commit_author_row:
                # logger.info('Url: {}, Author: {}, Commit count: {}'.format( key, row['author'], row['cnt']))
                # self.collection_of_forks_details.append([url,key, revisions_behind_finding_fork_url[key], row['author'], row['cnt']])
                self.collection_of_forks_details.append([url,key, row['author'], row['cnt']])

            # logger.info('Url : {} ended'.format(key))

        logger.info('Calculated revision per author count of fork urls of origin {}'.format(url))
        return self.writing_origin_fork_author_commit_to_csv(url)


    def writing_origin_fork_author_commit_to_csv(self,url):
        with open(url.split('/')[-1]+'.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerow(['Origin_url', 'Fork_url', 'Author', 'No. of Commit'])
            # csvwriter.writerow(['Origin_url', 'Fork_url', 'Main commits', 'Author', 'No. of Commit'])
            for i in self.collection_of_forks_details:
                csvwriter.writerow(i)
            self.collection_of_forks_details.clear()
            
        return 'Success retrieval of Url: '+ url




def main():
    no_of_url = int(input('Enter the number of origin url: '))
    url = []
    for i in range(no_of_url):
        url.append(input('Enter URL: '))
        print(url[i])
    
    for i in url:
        FindingForks().get_fork_with_authors(i)

    connection.close()

if __name__ == '__main__':
    main()