import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn


def execute_sql(conn, sql):
    """Execute sql
    :param conn: Connection object
    :param sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def add_autor(conn, autor):
    """
    Create a new autor into the autors table
    :param conn:
    :param autor:
    :return: autor id
    """
    sql = """INSERT INTO autors(first_name, last_name)
             VALUES(?,?)"""
    cur = conn.cursor()
    cur.execute(sql, autor)
    return cur.lastrowid


def add_book(conn, book):
    """
    Create a new book into the books table
    :param conn:
    :param book:
    :return: book id
    """
    sql = """INSERT INTO books(autor_id, title, genre, release_date_pl)
             VALUES(?,?,?,?)"""
    cur = conn.cursor()
    cur.execute(sql, book)
    conn.commit()
    return cur.lastrowid


def select_all(conn, table):
    """
    Query all rows in the table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    return rows


def select_where(conn, table, **query):
    """
    Query tasks from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    return rows


def update(conn, table, id, **kwargs):
    """
    update status, begin_date, and end date of a task
    :param conn:
    :param table: table name
    :param id: row id
    :return:
    """
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id,)

    sql = f""" UPDATE {table}
             SET {parameters}
             WHERE id = ?"""
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("OK")
    except sqlite3.OperationalError as e:
        print(e)


def delete_all(conn, table):
    """
    Delete all rows in the table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {table}")
    conn.commit()


def delete_where(conn, table, **query):
    """
    Delete row from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"DELETE FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    return rows


if __name__ == "__main__":
    create_autors_sql = """
    CREATE TABLE IF NOT EXISTS autors (
      id integer PRIMARY KEY,
      first_name VARCHAR(50) NOT NULL,
      last_name VARCHAR(50) NOT NULL
   );
   """

    create_books_sql = """
    CREATE TABLE IF NOT EXISTS books (
      id integer PRIMARY KEY,
      autor_id integer NOT NULL,
      title VARCHAR(100) NOT NULL,
      genre VARCHAR(20) NOT NULL,
      release_date_pl YEAR NOT NULL,
      FOREIGN KEY (autor_id) REFERENCES autors (id)
   );
   """

    db_file = "database.db"
    conn = create_connection(db_file)
    # creating tabels and filling with data
    if conn is not None:
        execute_sql(conn, create_autors_sql)
        execute_sql(conn, create_books_sql)

    autor_id_01 = add_autor(conn, ("Stieg", "Larsson"))
    add_book(
        conn, (autor_id_01, "Mężczyźni, którzy nienawidzą kobiet", "kryminał", "2008")
    )
    add_book(
        conn, (autor_id_01, "Dziewczyna, która igrała z ogniem", "kryminał", "2009")
    )
    add_book(conn, (autor_id_01, "Zamek z piasku, który runął", "kryminał", "2008"))
    autor_id_02 = add_autor(conn, ("Jo", "Nesbo"))
    add_book(conn, (autor_id_02, "Pierwszy śnieg", "kryminał", "2007"))
    add_book(conn, (autor_id_02, "Czerwone gardło", "kryminał", "2006"))
    add_book(conn, (autor_id_02, "Upiory", "kryminał", "2012"))
    autor_id_03 = add_autor(conn, ("Douglas", "Adams"))
    add_book(conn, (autor_id_03, "Autostopem przez galaktykę", "komedia", "1979"))
    autor_id_04 = add_autor(conn, ("Neil", "Gaiman"))
    add_book(conn, (autor_id_04, "Amerykańscy bogowie", "fantasy", "2002"))
    add_book(conn, (autor_id_04, "Koralina", "fantasy", "2003"))

    # Select data
    books_from_autor = select_where(conn, "books", autor_id=2)
    print(*books_from_autor, sep="\n")

    autors_all = select_all(conn, "autors")
    print(*autors_all, sep="\n")

    # Update
    update(conn, "autors", 4, first_name="Neil Richard")
    autor_from_autors = select_where(conn, "autors", id=4)
    print(autor_from_autors)

    # Delete
    delete_where(conn, "books", id=9)

    conn.commit()
    conn.close()
