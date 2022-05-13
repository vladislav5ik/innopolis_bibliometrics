import csv
import psycopg2
import os
from flask import Flask, request, send_from_directory, abort
from werkzeug.utils import secure_filename

app = Flask(__name__)
config = {
    "UPLOAD_PATH": os.path.join(app.root_path, 'uploads'),
    "UPLOAD_EXTENSIONS": ['csv']
}


def get_connection():
    if os.environ.get('DATABASE_URL') is None:
        # Creds on local machine
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
                          row["Authors"].split(","),
                          row["Authors with affiliations"].split(";")
                          )
            conn.commit()

            for author_i, author in enumerate(authors):
                sql_author = '''INSERT INTO authors (id, name, affiliation, is_innopolis) 
                                    VALUES (%s, %s, %s, %s) on conflict do nothing '''
                cur.execute(sql_author,
                            (author[0].strip(), author[1].strip(), author[2].strip(), 'innopolis' in author[2].lower()))
                conn.commit()

                sql_author_paper = '''INSERT INTO author_paper (paper_eid, author_id, is_primary) 
                                        VALUES (%s, %s, %s)'''
                cur.execute(sql_author_paper,
                            (row['EID'].strip(), author[0].strip(), author_i == 0))

                conn.commit()


def add_csv_to_html(cur, conn, output_file_path):
    sql = """Select name, affiliation
               from authors
               where is_innopolis = true
               order by name;"""
    cur.execute(sql)
    res = f'''<p><p><form action = download/{os.path.relpath(output_file_path, config['UPLOAD_PATH'])} method = "POST">
         <input type = "submit" value = "Download csv - Innopolis authors">
      </form>
      <p><p> Table of all Innopolis authors
        <table>
      <tr>
        <th>Author</th>
        <th>Affiliation</th>
      </tr>'''
    with open(output_file_path, mode='w', encoding='utf-8-sig', newline='') as csv_file:
        fieldnames = ['Author name', 'Affiliation']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for author in cur.fetchall():
            writer.writerow({'Author name': author[0], 'Affiliation': author[1]})
            #print({'Author name': author[0], 'Affiliation': author[1]})
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

        'Total authors who have ever co-authored':
            '''select count(distinct author_id)
               from author_paper
               where is_primary = false;''',

        'Authors <b>from Innopolis</b> who have ever co-authored':
            '''select count(distinct author_id)
               from author_paper
               join authors a on a.id = author_paper.author_id
               where is_primary = false and a.is_innopolis = true;''',

        'Total authors who have ever been the primary':
            '''select count(distinct author_id)
               from author_paper
               where is_primary = true;''',

        'Authors <b>from Innopolis</b> who have ever been the primary':
            '''select count(distinct author_id)
               from author_paper
               join authors a on a.id = author_paper.author_id
               where is_primary = true and a.is_innopolis = true;''',
    }
    res = ''
    for query_name, query in sql_dict.items():
        cur.execute(query)
        for item in cur.fetchall():
            res += f'<p> {query_name} = {item[0]}'

    return res


def analyze_csv(input_file_path, output_file_path):
    conn = get_connection()
    cur = conn.cursor()
    drop_tables(cur, conn)
    create_db(cur, conn)
    fill_db(input_file_path, cur, conn)
    results = analyze(cur, conn)
    results += add_csv_to_html(cur, conn, output_file_path)
    cur.close()
    conn.close()
    return results


@app.route('/upload', methods=['POST'])
def upload():
    if request.method == 'POST':

        input_file_path = os.path.join(config["UPLOAD_PATH"], 'file.csv')
        if os.path.exists(input_file_path):
            os.remove(input_file_path)
        output_file_path = os.path.join(config["UPLOAD_PATH"], 'innopolis_authors.csv')
        if os.path.exists(output_file_path):
            os.remove(output_file_path)

        f = request.files['file']
        f.save(input_file_path)
        result = analyze_csv(input_file_path, output_file_path)
        return result


@app.route('/download/<path:path>', methods=['POST'])
def download_file(path):
    if not path.endswith('.csv'):
        abort(405)
    return send_from_directory(directory=config['UPLOAD_PATH'],
                               path=path,
                               as_attachment=True)


@app.route('/')
def index():
    return '''<p>Please upload your csv file from scopus and click 'submit'<p>
    <form action="/upload" method="POST" enctype="multipart/form-data">
                <input type="file" name="file">
                <input type="submit" value="Submit">
              </form>'''


if __name__ == '__main__':
    if not os.path.exists(config["UPLOAD_PATH"]):
        os.makedirs(config["UPLOAD_PATH"])
    app.run(debug=True)
