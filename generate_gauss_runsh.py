import pathlib
import yaml

with open("config.yaml", "r") as f:
    settings = yaml.safe_load(f)

RELATIVE_DIR = settings['relative_dir']
JOB_DIR = settings["JOB_DIR"]
WORKING_DIR = settings['working_dir']
TEMPLATE = settings['template']


def write_jobsh_file(gjf_files, jobs_per_file=8, jobs_directory=pathlib.Path("jobs")):
    print(f"number of gjf files: {len(gjf_files)}")
    if not jobs_directory.exists():
        jobs_directory.mkdir()
    srun_strings = ''
    for i, gjf in enumerate(gjf_files):
        srun_strings += f'srun -n 1 -N 1 -c 16 --exclusive g09< {RELATIVE_DIR}/{gjf.name}> {gjf.stem}.log & \n'
        if i % jobs_per_file == jobs_per_file - 1:
            srun_strings += "wait\n"
            with open(f"{jobs_directory}/{i+1}.sh", "w") as f:
                f.write(TEMPLATE.format(srun_strings))
            srun_strings = ""
        elif i == len(gjf_files) - 1:
            srun_strings += "wait\n"
            with open(f"{jobs_directory}/{i+1}.sh", "w") as f:
                f.write(TEMPLATE.format(srun_strings))


def write_hpc_file(batch, jobs_directory, hpc_directory=pathlib.Path("hpc")):
    jobs_name = list(sorted(jobs_directory.glob("*.sh"), key=lambda x: int(x.stem)))
    print(f"number of jobs: {len(jobs_name)}")
    quotient, remainder = divmod(len(jobs_name), batch)
    if not hpc_directory.exists():
        hpc_directory.mkdir()
    i = 0
    for i in range(quotient):
        with open(f"{hpc_directory}/batch_{i}.txt", 'w') as f:
            for job in jobs_name[i*batch:(i+1)*batch]:
                f.write(f"sbatch -n 8 -N 1 -c 16 -D {WORKING_DIR}/{i} --mem=128GB {JOB_DIR}/{job.name}\n")
    if remainder != 0:
        with open(f"{hpc_directory}/batch_{i}.txt", 'w') as f:
            for job in jobs_name[quotient*batch:]:
                f.write(f"sbatch -n 8 -N 1 -c 16 -D {WORKING_DIR}/{i} --mem=128GB {JOB_DIR}/{job.name}\n")


# if __name__ == "__main__":
#     batch = 5000
#     jobs_per_file = 8
#     gjf_files = sorted(list(pathlib.Path("../Q10-1").glob("*.gjf")))
#     jobs_directory = pathlib.Path("jobs")
#     if not jobs_directory.exists():
#         jobs_directory.mkdir()
#     write_jobsh_file(gjf_files, jobs_per_file=jobs_per_file, jobs_directory=jobs_directory)
#     write_hpc_file(batch, jobs_directory=jobs_directory)
