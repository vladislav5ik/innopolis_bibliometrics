from flask import Flask, request, send_from_directory, abort, render_template
import psycopg2
from psycopg2.extras import execute_values
import time
import csv
import os

app = Flask(__name__)
config = {
    "UPLOAD_PATH": os.path.join(app.root_path, 'uploads'),
    "UPLOAD_EXTENSIONS": ['csv']
}

CONN = None
glob_test: list[list] = []


def get_or_create_connection():
    global CONN
    if CONN is None or CONN.closed:
        if os.environ.get('DATABASE_URL') is None:
            # Credentials on local machine
            cred = {
                "dbname": "scopus_test",
                "user": "postgres",
                "password": "postgres",
                "host": "localhost",
                "port": "5432"
            }
            CONN = psycopg2.connect(**cred)
        else:
            # If deployed on heroku
            database_url = os.environ['DATABASE_URL']
            CONN = psycopg2.connect(database_url, sslmode='require')
    return CONN, CONN.cursor()


def drop_tables():
    conn, cur = get_or_create_connection()
    sql = '''
            DROP TABLE IF EXISTS papers CASCADE;
            DROP TABLE IF EXISTS authors CASCADE;
            DROP TABLE IF EXISTS author_paper CASCADE;
            '''
    cur.execute(sql)
    conn.commit()


def create_schema():
    conn, cur = get_or_create_connection()
    sql = '''
            CREATE TABLE IF NOT EXISTS papers (
              eid varchar(255) primary key,
              title varchar(1000) not null,
              year integer not null,
              source_title varchar(1000),
              cited_by integer,
              doi varchar(255),
              link varchar(1000),
              source varchar(1000),
              innopolis_authors_count integer not null
            );
            
            CREATE TABLE IF NOT EXISTS authors (
              id varchar(255) primary key,
              name varchar(255)
            );
            
            
            CREATE TABLE IF NOT EXISTS author_paper (
              id serial primary key,
              paper_eid varchar(255) references papers(eid),
              author_id varchar(255) references authors(id),
              affiliation varchar(1000),
              is_innopolis boolean,
              is_primary boolean
            );
          '''
    cur.execute(sql)
    conn.commit()


def fill_db(file):
    list_papers = []
    list_authors = []
    list_author_papers = []
    list_points = dict()

    with open(file, mode='r', encoding='utf-8-sig') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            authors = list(zip(row["Author(s) ID"].split(";"),
                               row["Authors"].split(","),
                               [''.join(affiliation.split('.,')[1:]) for affiliation in
                                row["Authors with affiliations"].split(";")]
                               )
                           )
            paper_data = (row['EID'],
                          row['Title'],
                          row['Year'],
                          row['Source title'],
                          0 if not row['Cited by'] else row['Cited by'],  # cited_by (number of citations)
                          row['DOI'],
                          row['Link'],
                          row['Source'],
                          sum([is_innopolis_affiliation(author[2]) for author in authors])  # authors count
                          )
            list_papers.append(paper_data)

            for author_i, author in enumerate(authors):
                author_data = (author[0].strip(),  # id
                               author[1].strip(),  # name
                               )
                list_authors.append(author_data)

                author_paper_data = (row['EID'].strip(),
                                     author[0].strip(),
                                     author[2].strip(),  # affiliation
                                     is_innopolis_affiliation(author[2]),  # is_innopolis
                                     author_i == 0  # is_primary
                                     )
                list_author_papers.append(author_paper_data)
                if is_innopolis_affiliation(author[2]):
                    points: float = 1 / sum([is_innopolis_affiliation(author_2[2]) for author_2 in authors])
                    if author[0].strip() in list_points:
                        list_points[author[0].strip()] += points
                    else:
                        list_points[author[0].strip()] = points
    sql_paper = '''INSERT INTO papers (eid, title, year, source_title,
                                       cited_by, doi, link, source, innopolis_authors_count) 
                   VALUES %s'''
    sql_author = '''INSERT INTO authors (id, name) 
                    VALUES %s on conflict do nothing '''
    sql_author_paper = '''INSERT INTO author_paper (paper_eid, author_id, affiliation, is_innopolis, is_primary) 
                          VALUES %s'''
    conn, cur = get_or_create_connection()
    execute_values(cur, sql_paper, list_papers)
    execute_values(cur, sql_author, list_authors)
    execute_values(cur, sql_author_paper, list_author_papers)
    conn.commit()
    global glob_test
    glob_test = sorted(list_points.items(), key=lambda x: x[0])
    print(sum(list_points.values()))


def is_innopolis_affiliation(affiliation: str):
    return 'innopolis' in affiliation.lower()


def create_csv_output(output_file_path):
    conn, cur = get_or_create_connection()
    sql_innopolis_affiliations = """select
        author_paper.author_id as author_id,
        array_to_string(array_agg(distinct authors.name),'; ') AS author_name,
        array_to_string(array_agg(distinct affiliation),'; ') AS author_affiliations,
        count(distinct author_paper.paper_eid) as paper_count,
        sum (1.0 :: decimal / papers.innopolis_authors_count :: decimal)::decimal as points
        from author_paper
        join papers on author_paper.paper_eid = papers.eid
        join authors on author_paper.author_id = authors.id
        where author_paper.is_innopolis
        group by author_paper.author_id
        order by author_paper.author_id;"""

    preview_list = []
    cur.execute(sql_innopolis_affiliations)
    with open(output_file_path, mode='w', encoding='utf-8-sig', newline='') as csv_file:
        fieldnames = ['Author ID', 'Author name', 'Affiliation', 'Number of publications', 'Points', 'test']
        preview_list.append(fieldnames)
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row_number, author in enumerate(cur.fetchall()):
            row = dict()
            for field_number, fieldname in enumerate(fieldnames):
                if fieldname == 'test':
                    row[fieldname] = glob_test[row_number]
                    continue
                row[fieldname] = author[field_number]
            writer.writerow(row)

            # preview limit
            # if row_number < 10:
            preview_list.append(list(row.values()))
    return preview_list


def analyze():
    conn, cur = get_or_create_connection()
    sql_dict = {
        'Total papers':
            '''Select count(*)
               from papers;''',

        'Total authors':
            '''Select count(*)
               from authors;''',

        'Authors from Innopolis':
            '''select count(distinct author_id)
               from author_paper
               where is_innopolis = true;''',

        'Authors not from Innopolis':
            '''select count(distinct author_id)
               from author_paper
               where is_innopolis = false;''',

        'Total authors who have ever co-authored':
            '''select count(distinct author_id)
               from author_paper
               where is_primary = false;''',

        'Authors from Innopolis who have ever co-authored':
            '''select count(distinct author_id)
               from author_paper
               where is_primary = false and is_innopolis = true;''',

        'Total authors who have ever been the primary':
            '''select count(distinct author_id)
               from author_paper
               where is_primary = true;''',

        'Authors from Innopolis who have ever been the primary':
            '''select count(distinct author_id)
               from author_paper
               where is_primary = true and is_innopolis = true;''',
    }
    res = {}
    for query_name, query in sql_dict.items():
        cur.execute(query)
        for item in cur.fetchall():
            res[query_name] = item[0]

    return res


@app.route('/upload', methods=['POST'])
def upload():
    """This handler is used to upload a file to the server and then process it.
    The file is saved in the UPLOAD_PATH directory.
    The result of the processing is returned to the user with some analytics data."""
    if request.method == 'POST':
        os.makedirs(config["UPLOAD_PATH"], exist_ok=True)
        timestamp = time.time()
        input_file_path = os.path.join(config["UPLOAD_PATH"], f'upload-{timestamp}.csv')
        output_file_path = os.path.join(config["UPLOAD_PATH"], f'result-{timestamp}.csv')
        f = request.files['file']
        f.save(input_file_path)

        drop_tables()
        create_schema()
        fill_db(input_file_path)
        preview_list = create_csv_output(output_file_path)
        analytics_dict = analyze()

        return render_template('upload.html',
                               analytics_dict=analytics_dict,
                               preview_list=preview_list,
                               output_file_name=os.path.relpath(output_file_path, config['UPLOAD_PATH']))


@app.route('/download/<path:path>', methods=['POST'])
def download_file(path):
    """This handler is used to download a file from the server."""
    if not path.split('.')[-1] in config['UPLOAD_EXTENSIONS']:  # The file is only downloadable if it is a csv file.
        abort(405)
    return send_from_directory(directory=config['UPLOAD_PATH'],
                               path=path,
                               mimetype='application/x-csv',
                               download_name='result.csv',
                               as_attachment=True)


@app.route('/')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)
