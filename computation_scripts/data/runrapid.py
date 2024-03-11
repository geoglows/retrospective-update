import datetime
import glob
import os
import subprocess
from multiprocessing import Pool
import argparse

def timestamp():
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %X')

def run_rapid_for_namelist_directory(path_to_rapid_executable: str,
                                     namelist_dir: str, ) -> None:
    today = datetime.datetime.today()
    for namelist in namelist_dir:
        with open(f'/mnt/data/logs/{os.path.basename(namelist).split("_")[1]}_{today.year}_{today.month}_{today.day}.log', 'w') as f:
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
    # path_to_rapid_exec = '/home/rapid/src/rapid'
    # namelist_dir = '/mnt/data/namelists'

    namelists = glob.glob(os.path.join(namelist_dir, '*namelist*'))

    cpu_count = min([os.cpu_count(), len(namelists)])
    print(f'Found {len(namelists)} input directories')
    print(f'Have {os.cpu_count()} cpus')
    print(f'Using {cpu_count} cpus')
    namelists = [namelists[i:i+cpu_count] for i in range(0, len(namelists), cpu_count)]

    os.makedirs('/mnt/data/logs/', exist_ok=True)
    with Pool(cpu_count) as p:
        p.starmap(run_rapid_for_namelist_directory, [(path_to_rapid_exec, d) for d in namelists])

    print('Finished RAPID')
