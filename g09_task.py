import time
import sqlite3
from pathlib import Path
import tqdm

conn = sqlite3.connect('tasks.db')
cursor = conn.cursor()


def create_table():
    cursor.execute('''
        CREATE TABLE g09_task (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inchi_key TEXT NOT NULL,
            batch TEXT NOT NULL,
            conformation_id TEXT NOT NULL,
            gjf_file TEXT NOT NULL,
            log_file TEXT NOT NULL,
            chk_file TEXT NOT NULL,
            status TEXT CHECK( status IN ('success','fail','unsubmit') ) NOT NULL,
            created_at TEXT NOT NULL,
            comment TEXT NULL
        )
    ''')


# 函数用于插入数据
def insert_data(inchi_key, conformation_id, gjf_file, log_file, chk_file, status, batch, date):
    cursor.execute('''
        INSERT INTO g09_task (inchi_key, batch, conformation_id, gjf_file, log_file, chk_file, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (inchi_key, batch, conformation_id, gjf_file, log_file, chk_file, status, date))

# 函数用于更新数据


def commit_and_close():
    conn.commit()
    conn.close()


def update_data(inchi_key, conformation_id, log_file, chk_file, status):
    cursor.execute('''
        UPDATE g09_task
        SET log_file = ?, chk_file = ?, status = ?
        WHERE inchi_key = ? AND conformation_id = ?
        ''', (log_file, chk_file, status, inchi_key, conformation_id))

def update_batch_data(inchi_keys, ids, log_files, chk_files, sts):
    conn.execute("BEGIN TRANSACTION")
    for inchi_key, id, log_file, chk_file, status in tqdm.tqdm(zip(inchi_keys, ids, log_files, chk_files, sts), total=len(sts)):
        update_data(inchi_key, id, log_file, chk_file, status)


def insert_batch_data(batch: str, directory: Path):
    status = 'unsubmit'
    date = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    for file in directory.glob("*.gjf"):
        gjf_file = str(file)
        log_file = ""
        chk_file = ""
        inchi_key = file.name.split("_freqcharge_")[0]
        conformation_id = file.stem.split("_")[-1]
        insert_data(inchi_key, conformation_id, gjf_file, log_file, chk_file, status, batch, date)


def failed_gjf_file():
    cursor.execute('''SELECT gjf_file FROM conformations WHERE status = 'fail' ''')
    return cursor.fetchall()

