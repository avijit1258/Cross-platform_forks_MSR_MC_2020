from pydriller import RepositoryMining
import csv
import logging
import pandas as pd
import matplotlib.pyplot as plt



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
def read_from_github(url, dict):

    """
    url: github link of a repository
    dict: list commit id, date and message where id is key and other two are value
    purpose: This compares a github repositories all commits from url with the list of commits in dict
    """
    commit_count = 0
    revision = 0
    for commit in RepositoryMining(url).traverse_commits():
        commit_count += 1
        if commit.hash in dict:
            revision += 1
            print('COMMIT Count: {}, Id : {}, Date : {}, Message : {}'.format(commit_count,commit.hash, commit.committer_date, commit.msg))

    print(revision)

def read_csv(file):

    """
    Reads a csv file with id, date and message

    Returns a dictionary where id is used as key and date,message are value pairs.

    """

    id = []
    date = []
    message = []
    with open('revisions.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            line_count += 1
            id.append(row['id'])
            date.append(row['date'])
            message.append(row['message'])

    csv_to_dict = {id[i]: [date[i], message[i]] for i in range(len(id))}

    return csv_to_dict


def comparing_two_repository_from_url(main_url, fork_url):
    """
        Retrieves commits from two url and then compares them pairwise.

        Return: Prints common and mismatch commits.
    """

    main_url_commits = {commit.hash: [commit.author_date, commit.msg] for commit in RepositoryMining(main_url).traverse_commits()}

    fork_url_commits = {commit.hash: [commit.author_date, commit.msg] for commit in RepositoryMining(fork_url).traverse_commits()}
    commit_count = 0
    for key in main_url_commits:
        commit_count += 1
        logger.info('Commit count main: {}'.format(commit_count))
        if key in fork_url_commits:
            logger.info('Main Commit count: {}, ID: {}, Date: {}, Message: {}'.format(commit_count, key,\
                                                                                      main_url_commits[key][0], main_url_commits[key][1]))
    commit_count = 0
    for key in fork_url_commits:
        commit_count += 1
        logger.info('Commit count main: {}'.format(commit_count))
        if key in main_url_commits:
            logger.info('Main Commit count: {}, ID: {}, Date: {}, Message: {}'.format(commit_count, key, \
                                                                                      fork_url_commits[key][0], fork_url_commits[key][1]))

    return

def finding_users_in_forks(filename):
    """
    filename: contains origin url, fork url with their unique author and their author count
    fork_to_users: counts unique commit users in forks and returns a dict as fork url to user count map
    """

    fork_to_users = {}

    with open(filename, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if row['Fork_url'] in fork_to_users:
                fork_to_users[row['Fork_url']] += 1
            else:
                fork_to_users[row['Fork_url']] = 1

    return fork_to_users

def counting_boxplot_info_from_fork_user_dict(fork_user):

    """
    fork_user: Takes a repository to user_count dictionary map 
    purpose : returns boxplot required information
    """

    df = pd.DataFrame.from_dict(fork_user,orient='index', columns=['user_count'])

    print(df.user_count.describe())

    B = plt.boxplot(df['user_count'])
    print([item.get_ydata()[1] for item in B['whiskers']])
    # plt.show()

    return [item.get_ydata()[1] for item in B['whiskers']]

def finding_gitlab_forks(fork_user):
    """
    fork_user: Takes a repository to user_count dictionary map
    purpose: calculates how much gitlab forks are there among all the forks    
    """
    total_forks = 0
    gitlab_forks = 0
    gitlab_url = []
    for fu in fork_user:
        total_forks += 1
        if 'https://gitlab.com/' in fu:
            gitlab_url.append(fu)
            gitlab_forks += 1

    return total_forks, gitlab_forks, gitlab_url

def calculating_forks_category(fork_user):
    """
    fork_user: Takes a repository to user_count dictionary map
    purpose: calculates different categories of fork we have created 3 different categories    
    """

    personal, small_community, large_community = 0, 0, 0
    for i in fork_user:
        if fork_user[i] <= 1:
            personal += 1
        elif fork_user[i] <= 25:
            small_community += 1
        elif fork_user[i] >= 26:
            large_community += 1

    return personal, small_community, large_community


def main():
    # read_from_github('https://github.com/zzzeek/sqlalchemy', read_csv('revisions.csv'))
    # 'https://github.com/vim/vim', 'https://github.com/neovim/neovim'
    # 'https://github.com/PSeminarKryptographie/MonkeyCrypt', 'https://github.com/avijit1258/MonkeyCrypt'
    # comparing_two_repository_from_url('https://github.com/vim/vim', 'https://github.com/lonetwin/vim')
    # counting_boxplot_info_from_fork_user_dict(finding_users_in_forks('dataset/vim.csv'))
    subject_systems = ['bitcoin.csv', 'node.csv','flutter.csv', 'neovim.csv', 'react-native.csv', 'scikit-learn.csv', 'spaCy.csv', 'tensorflow.csv', 'TextBlob.csv', 'vim.csv']
    f = open("gitlab_forks.txt", "w")
    f.write("GitHub, Number of GitLab Forks ,GitLab Forks\n")
    
    for i in subject_systems:
        
        total, gitlab, gitlab_url = finding_gitlab_forks(finding_users_in_forks('final_paper/'+i))
        # print('System: {}, Total fork: {}, GitLab fork {}, Url {}'.format(i, total, gitlab, gitlab_url))
        print('System: {}, Total fork: {}, GitLab fork {}'.format(i, total, gitlab))
        f.write('{}, {}, {}\n'.format(i, gitlab, gitlab_url))
        # c1, c2, c3 = calculating_forks_category(finding_users_in_forks('dataset/'+i))
        # print('System: {}, Personal: {}, Small Community: {}, Large Community: {}'.format(i, (c1/total) * 100, (c2/total) * 100, (c3/total)* 100))
    f.close()

if __name__ == '__main__':
    main()