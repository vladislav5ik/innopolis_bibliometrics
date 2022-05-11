import csv
import psycopg2
import os
from flask import Flask, request
app = Flask(__name__)

def get_connection():
    if os.environ.get('DATABASE_URL') is None:
        cred = {
            "dbname": "scopus_test",
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": "5432"
        }
        connection = psycopg2.connect(**cred)
    else:
        # If deployed on heroku
        DATABASE_URL = os.environ['DATABASE_URL']
        connection = psycopg2.connect(DATABASE_URL, sslmode='require')
    return connection

def drop_tables(cur, conn):
    sql = '''
            DROP TABLE IF EXISTS papers CASCADE;
            DROP TABLE IF EXISTS authors CASCADE;
            DROP TABLE IF EXISTS author_paper CASCADE;
            '''
    cur.execute(sql)
    conn.commit()

def create_db(cur, conn):
    sql = '''
            CREATE TABLE IF NOT EXISTS papers (
              eid varchar(255) primary key,
              title varchar(1000) not null,
              year integer not null,
              source_title varchar(1000),
              cited_by integer,
              doi varchar(255),
              link varchar(1000),
              source varchar(1000)
            );
            
            CREATE TABLE IF NOT EXISTS authors (
              id varchar(255) primary key,
              name varchar(255),
              affiliation varchar(1000),
              is_innopolis boolean
            );
            
            CREATE TABLE IF NOT EXISTS author_paper (
              id serial primary key,
              paper_eid varchar(255) references papers(eid),
              author_id varchar(255) references authors(id),
              is_primary boolean
            );
          '''
    cur.execute(sql)
    conn.commit()


def fill_db(file, cur, conn):
    with open(file, mode='r', encoding='utf-8-sig') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            sql_paper = '''INSERT INTO papers (eid, title, year, source_title, cited_by, doi, link, source) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
            cur.execute(sql_paper,
                        (row['EID'], row['Title'], row['Year'], row['Source title'],
                         0 if not row['Cited by'] else row['Cited by'], row['DOI'],
                         row['Link'], row['Source']))

            authors = zip(row["Author(s) ID"].split(";"),
                          row["Authors"].split(", "),
                          row["Affiliations"].split("; ")
                          )
            conn.commit()

            for author_i, author in enumerate(authors):
                sql_author = '''INSERT INTO authors (id, name, affiliation, is_innopolis) 
                                    VALUES (%s, %s, %s, %s) on conflict do nothing '''
                cur.execute(sql_author,
                            (author[0], author[1], author[2], 'Innopolis' in author[2]))
                conn.commit()

                sql_author_paper = '''INSERT INTO author_paper (paper_eid, author_id, is_primary) 
                                        VALUES (%s, %s, %s)'''
                cur.execute(sql_author_paper,
                            (row['EID'], author[0], author_i == 0))

                conn.commit()

def add_csv_to_html(cur, conn):
    sql = """Select name, affiliation
               from authors
               where is_innopolis = true
               order by name;"""
    cur.execute(sql)
    res = '''<p><p> Table of all Innopolis authors
        <table>
      <tr>
        <th>Author</th>
        <th>Affiliation</th>
      </tr>'''
    for author in cur.fetchall():
        res += f"""
      <tr>
        <td>{author[0]}</td>
        <td>{author[1]}</td>
      </tr> """
    res += """</table>"""
    return res
def analyze(cur, conn):
    sql_dict = {
        'Total papers':
            '''Select count(*)
               from papers;''',

        'Total authors':
            '''Select count(*)
               from authors;''',

        'Authors <b>from Innopolis</b>':
            '''Select count(*)
               from authors
               where is_innopolis = true;''',

        'Authors <b> not </b> from Innopolis':
            '''Select count(*)
               from authors
               where is_innopolis = false;''',

        'Total authors who co-authored':
            '''select count(distinct author_id)
               from author_paper
               where is_primary = false;''',

        'Authors <b>from Innopolis</b> who co-authored':
            '''select count(distinct author_id)
               from author_paper
               join authors a on a.id = author_paper.author_id
               where is_primary = false and a.is_innopolis = true;''',

        'Total authors who were the primary':
            '''select count(distinct author_id)
               from author_paper
               where is_primary = true;''',

        'Authors <b>from Innopolis</b> who were the primary':
            '''select count(distinct author_id)
               from author_paper
               join authors a on a.id = author_paper.author_id
               where is_primary = true and a.is_innopolis = true;''',

        #'How many authors were published with other co-authors':
        #    '''SELECT count(DISTINCT author_id)
        #       FROM author_paper
        #       WHERE is_primary = true AND
        #       paper_eid IN (
        #         SELECT paper_eid
        #         FROM author_paper
        #         WHERE is_primary = false
        #       )''',
        #'How many authors were published with other co-authors from Innopolis':
        #    '''SELECT count(DISTINCT author_id)
        #       FROM author_paper
        #       WHERE is_primary = true AND
        #       paper_eid IN (
        #         SELECT paper_eid
        #         FROM author_paper
        #         join authors a on a.id = author_paper.author_id
        #         WHERE is_primary = false and a.is_innopolis = true
        #       )'''
    }
    res = ''
    for query_name, query in sql_dict.items():
        cur.execute(query)
        for item in cur.fetchall():
            res += f'<p> {query_name} = {item[0]}'

    res += add_csv_to_html(cur, conn)
    return res

# List of authors in Innopolis (author ID, author name, author affiliation)
# List of papers of each author (author ID, author name, list of DOI of papers)
# List of unknown authors, with no affiliation field (author ID, author name)

@app.route('/')
def index():
    return '''<form action="/upload" method="POST" enctype="multipart/form-data">
                <input type="file" name="file">
                <input type="submit" value="Submit">
              </form>'''

@app.route('/upload', methods=['POST'])
def upload():
    if request.method == 'POST':
        f = request.files['file']
        filename = csv_download(f)
        return analyze_csv(filename)


def csv_download(f):
    filename = './file.csv'
    f.save(filename)
    return filename


def analyze_csv(filename):
    conn = get_connection()
    cur = conn.cursor()
    drop_tables(cur, conn)
    create_db(cur, conn)
    file = 'csv_files/scopus.csv' if filename is None else filename
    fill_db(file, cur, conn)
    results = analyze(cur, conn)
    cur.close()
    conn.close()
    return results

if __name__ == '__main__':
    app.run(debug=True)