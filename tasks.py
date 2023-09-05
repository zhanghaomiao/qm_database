import os
import tqdm
from invoke import task
from g09_task import create_table, insert_batch_data, update_batch_data, failed_gjf_file, commit_and_close
from generate_gauss_runsh import write_jobsh_file, write_hpc_file
from pathlib import Path


def last_line(file):
    with open(file, 'rb') as f:
        f.seek(-2, os.SEEK_END)
        while f.read(1) != b'\n':
            f.seek(-2, os.SEEK_CUR)
        return f.readline().decode()


@task
def init_db(ctx):
    print("create table!")
    create_table()
    commit_and_close()


@task
def insert_data(ctx, directory):
    print("insert data!")
    directory = Path(directory).resolve()
    insert_batch_data(directory.name, directory)
    commit_and_close()


@task
def update_result(ctx, cal_dir):
    print("update result!")
    log_files = list(sorted([file for file in Path(cal_dir).glob("*.log") if os.path.getsize(file)]))
    inchi_keys, ids, sts, chk_files = [], [], [], []
    for file in tqdm.tqdm(log_files):
        if last_line(file).startswith("Normal termination of Gaussian 09"):
            sts.append("success")
        else:
            sts.append("fail")
        chk_file = file.with_suffix(".chk")
        if not os.path.exists(chk_file):
            chk_file = ""
        chk_files.append(chk_file)
        inchi_keys.append(file.stem.split("_freqcharge_")[0])
        ids.append(file.stem.split("_")[-1])
    update_batch_data(inchi_keys, ids, log_files, chk_files, sts)
    commit_and_close()


@task
def regenerate_failed_job(ctx):
    print("regenerate failed job!")
    gjfs = failed_gjf_file()
    write_jobsh_file(gjfs, jobs_per_file=8, jobs_directory=Path("regenerate_jobs"))
    write_hpc_file(5000, jobs_directory=Path("regenerate_jobs"), hpc_directory=Path("regenerate_hpc"))
