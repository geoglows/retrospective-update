import datetime
import glob
import os
import subprocess
from multiprocessing import Pool
import argparse

path_to_rapid_executable = '/home/rapid/src/rapid'


def timestamp():
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %X')


def run_rapid_for_namelist_directory(namelists: str, ) -> None:
    today = datetime.datetime.today()
    for namelist in namelists:
        with open(
                f'/mnt/data/logs/{os.path.basename(namelist).split("_")[1]}_{today.year}_{today.month}_{today.day}.log',
                'w') as f:
            f.write(f'{timestamp()}: Running RAPID for {namelist}')
            subprocess.call(
                [path_to_rapid_executable, '--namelist', namelist, '--ksp_type', 'preonly'],
                stdout=f,
                stderr=f
            )
            f.write(f'{timestamp()}: Finished RAPID for {namelist}')
    return


if __name__ == '__main__':
    """
    Run RAPID for all namelist files in a specified directory. This has been modified for use in AWS

    Usage:
        python run_rapid.py <path_to_rapid_executable> <namelists_dir>
 
    Directory structure:
        <namelist_directory>
            rapid_namelist_<watershed_1_id>
            rapid_namelist_<watershed_2_id>
            rapid_namelist_<watershed_3_id>
            ...

    Returns:
        None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--rapidexec', type=str, required=False,
                        default='/home/rapid/src/rapid',
                        help='Path to rapid executable', )
    parser.add_argument('--namelistsdir', type=str, required=False,
                        default='/mnt/namelists',
                        help='Path to directory containing namelist files', )

    args = parser.parse_args()
    path_to_rapid_exec = args.rapidexec
    namelist_dir = args.namelistsdir

    namelist_dirs = glob.glob(os.path.join(namelist_dir, '*'))
    namelist_jobs = [sorted(glob.glob(os.path.join(d, '*'))) for d in namelist_dirs]

    cpu_count = min([os.cpu_count(), len(namelist_jobs)])
    print(f'Found {len(namelist_dirs)} input directories')
    print(f'Found {len(namelist_jobs)} total namelist jobs')
    print(f'Have {os.cpu_count()} cpus')
    print(f'Using {cpu_count} cpus')

    os.makedirs('/mnt/data/logs/', exist_ok=True)
    with Pool(cpu_count) as p:
        p.map(run_rapid_for_namelist_directory, namelist_jobs)

    print('Finished RAPID')
