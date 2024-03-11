import glob
import os
import argparse
import netCDF4
import pandas as pd
import natsort
import logging
import sys

def rapid_namelist(
        namelist_save_path: str,

        k_file: str,
        x_file: str,
        riv_bas_id_file: str,
        rapid_connect_file: str,
        vlat_file: str,
        qout_file: str,

        time_total: int,
        timestep_calc_routing: int,
        timestep_calc: int,
        timestep_inp_runoff: int,

        # Optional - Flags for RAPID Options
        run_type: int = 1,
        routing_type: int = 1,

        use_qinit_file: bool = False,
        qinit_file: str = '',  # qinit_VPU_DATE.csv

        write_qfinal_file: bool = True,
        qfinal_file: str = '',

        compute_volumes: bool = False,
        v_file: str = '',

        use_dam_model: bool = False,  # todo more options here
        use_influence_model: bool = False,
        use_forcing_file: bool = False,
        use_uncertainty_quantification: bool = False,

        opt_phi: int = 1,

        # Optional - Can be determined from rapid_connect
        reaches_in_rapid_connect: int = None,
        max_upstream_reaches: int = None,

        # Optional - Can be determined from riv_bas_id_file
        reaches_total: int = None,

        # Optional - Optimization Runs Only
        time_total_optimization: int = 0,
        timestep_observations: int = 0,
        timestep_forcing: int = 0,
) -> None:
    """
    Generate a namelist file for a RAPID routing run

    All units are strictly SI: meters, cubic meters, seconds, cubic meters per second, etc.

    Args:
        namelist_save_path (str): Path to save the namelist file
        k_file (str): Path to the k_file (input)
        x_file (str): Path to the x_file (input)
        rapid_connect_file (str): Path to the rapid_connect_file (input)
        qout_file (str): Path to save the Qout_file (routed discharge file)
        vlat_file (str): Path to the Vlat_file (inflow file)

    Returns:
        None
    """
    assert run_type in [1, 2], 'run_type must be 1 or 2'
    assert routing_type in [1, 2, 3, ], 'routing_type must be 1, 2, 3, or 4'
    assert opt_phi in [1, 2], 'opt_phi must be 1, or 2'

    if any([x is None for x in (reaches_in_rapid_connect, max_upstream_reaches)]):
        df = pd.read_csv(rapid_connect_file, header=None)
        reaches_in_rapid_connect = df.shape[0]
        rapid_connect_columns = ['rivid', 'next_down', 'count_upstream']  # plus 1 per possible upstream reach
        max_upstream_reaches = df.columns.shape[0] - len(rapid_connect_columns)

    if reaches_total is None:
        df = pd.read_csv(riv_bas_id_file, header=None)
        reaches_total = df.shape[0]

    namelist_options = {
        'BS_opt_Qfinal': f'.{str(write_qfinal_file).lower()}.',
        'BS_opt_Qinit': f'.{str(use_qinit_file).lower()}.',
        'BS_opt_dam': f'.{str(use_dam_model).lower()}.',
        'BS_opt_for': f'.{str(use_forcing_file).lower()}.',
        'BS_opt_influence': f'.{str(use_influence_model).lower()}.',
        'BS_opt_V': f'.{str(compute_volumes).lower()}.',
        'BS_opt_uq': f'.{str(use_uncertainty_quantification).lower()}.',

        'k_file': f"'{k_file}'",
        'x_file': f"'{x_file}'",
        'rapid_connect_file': f"'{rapid_connect_file}'",
        'riv_bas_id_file': f"'{riv_bas_id_file}'",
        'Qout_file': f"'{qout_file}'",
        'Vlat_file': f"'{vlat_file}'",
        'V_file': f"'{v_file}'",

        'IS_opt_run': run_type,
        'IS_opt_routing': routing_type,
        'IS_opt_phi': opt_phi,
        'IS_max_up': max_upstream_reaches,
        'IS_riv_bas': reaches_in_rapid_connect,
        'IS_riv_tot': reaches_total,

        'IS_dam_tot': 0,
        'IS_dam_use': 0,
        'IS_for_tot': 0,
        'IS_for_use': 0,

        'Qinit_file': f"'{qinit_file}'",
        'Qfinal_file': f"'{qfinal_file}'",

        'ZS_TauR': timestep_inp_runoff,
        'ZS_dtR': timestep_calc_routing,
        'ZS_TauM': time_total,
        'ZS_dtM': timestep_calc,
        'ZS_TauO': time_total_optimization,
        'ZS_dtO': timestep_observations,
        'ZS_dtF': timestep_forcing,
    }

    # generate the namelist file
    namelist_string = '\n'.join([
        '&NL_namelist',
        *[f'{key} = {value}' for key, value in namelist_options.items()],
        '/',
        ''
    ])

    with open(namelist_save_path, 'w') as f:
        f.write(namelist_string)

def rapid_namelist_from_directories(vpu_directory: str,
                                    inflows_directory: str,
                                    namelists_directory: str,
                                    outputs_directory: str,
                                    datesubdir: bool = False,
                                    qinit_file: str = None,
                                    search_for_qinit_file: bool = True, ) -> None:
    vpu_code = os.path.basename(vpu_directory)
    k_file = os.path.join(vpu_directory, f'k.csv')
    x_file = os.path.join(vpu_directory, f'x.csv')
    riv_bas_id_file = os.path.join(vpu_directory, f'riv_bas_id.csv')
    rapid_connect_file = os.path.join(vpu_directory, f'rapid_connect.csv')

    for x in (k_file, x_file, riv_bas_id_file, rapid_connect_file):
        assert os.path.exists(x), f'{x} does not exist'

    inflow_files = natsort.natsorted(glob.glob(os.path.join(inflows_directory, '*.nc')))
    if not len(inflow_files):
        print(f'No inflow files found for VPU {vpu_code}')
        return

    os.makedirs(namelists_directory, exist_ok=True)

    for idx, inflow_file in enumerate(sorted(inflow_files)):
        inflow_file_name_params = os.path.basename(inflow_file).replace('.nc', '').split('_')
        start_date = inflow_file_name_params[2]
        end_date = inflow_file_name_params[3]
        file_label = inflow_file_name_params[4] if len(inflow_file_name_params) > 4 else ''

        namelist_file_name = f'namelist_{vpu_code}'
        namelist_file_name = f'namelist_{vpu_code}_{start_date}_{end_date}'
        namelist_file_name = f'namelist_{vpu_code}_{start_date}'
        qout_file_name = f'Qout_{vpu_code}_{start_date}_{end_date}.nc'
        vlat_file = inflow_file
        write_qfinal_file = True
        qfinal_file = os.path.join(outputs_directory, f'Qfinal_{vpu_code}_{end_date}.nc')

        if file_label:
            namelist_file_name += f'_{file_label}'
            qout_file_name = qout_file_name.replace('.nc', f'_{file_label}.nc')
            qfinal_file = qfinal_file.replace('.nc', f'_{file_label}.nc')

        namelist_save_path = os.path.join(namelists_directory, namelist_file_name)
        qout_path = os.path.join(outputs_directory, qout_file_name)
        os.makedirs(outputs_directory, exist_ok=True)

        if datesubdir:
            os.makedirs(os.path.join(outputs_directory, f'{start_date}'), exist_ok=True)
            qout_path = os.path.join(outputs_directory, f'{start_date}', qout_file_name, )

        with netCDF4.Dataset(inflow_file) as ds:
            time_step_inflows = ds['time_bnds'][0, 1] - ds['time_bnds'][0, 0]
            time_total_inflow = ds['time_bnds'][-1, 1] - ds['time_bnds'][0, 0]
        time_total = time_total_inflow
        timestep_inp_runoff = time_step_inflows
        timestep_calc = time_step_inflows
        timestep_calc_routing = 900

        # # todo
        # # scan the outputs directory for files matching the pattern Qfinal_VPU_CODE_YYYYMMDD.nc
        # possible_qinit_files = sorted(glob.glob(os.path.join(outputs_directory, f'Qfinal_{vpu_code}_*.nc')))
        # if len(possible_qinit_files):
        #     # use the most recent file
        #     qinit_file = possible_qinit_files[-1]
        # else:
        #     qinit_file = ''
        use_qinit_file = idx > 0 or qinit_file is not None
        # qinit_file = os.path.join(
        #     outputs_directory, f'Qfinal_{vpu_code}_{inflow_files[idx - 1].split("_")[-1]}'
        # ) if use_qinit_file else ''

        rapid_namelist(namelist_save_path=namelist_save_path,
                       k_file=k_file,
                       x_file=x_file,
                       riv_bas_id_file=riv_bas_id_file,
                       rapid_connect_file=rapid_connect_file,
                       vlat_file=vlat_file,
                       qout_file=qout_path,
                       time_total=time_total,
                       timestep_calc_routing=timestep_calc_routing,
                       timestep_calc=timestep_calc,
                       timestep_inp_runoff=timestep_inp_runoff,
                       write_qfinal_file=write_qfinal_file,
                       qfinal_file=qfinal_file,
                       use_qinit_file=use_qinit_file,
                       qinit_file=qinit_file, )

    return


if __name__ == '__main__':
    """
    Prepare rapid namelist files for a directory of VPU inputs
    """
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--basedir', type=str, required=True)
    argparser.add_argument('--dockerpaths', action='store_true', default=False)
    argparser.add_argument('--datesubdir', action='store_true', default=False)
    args = argparser.parse_args()

    base_dir = args.basedir
    dockerpaths = args.dockerpaths
    datesubdir = args.datesubdir

    base_dir = base_dir[:-1] if base_dir.endswith('/') else base_dir
    vpu_dirs = os.path.join(base_dir, 'inputs')
    inflow_dirs = os.path.join(base_dir, 'inflows')
    namelist_dirs = os.path.join(base_dir, 'namelists')
    output_dirs = os.path.join(base_dir, 'outputs')

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout,
    )

    all_vpu_dirs = sorted([x for x in glob.glob(os.path.join(vpu_dirs, '*')) if os.path.isdir(x)])
    for vpu_dir in all_vpu_dirs:
        inflow_dir = os.path.join(inflow_dirs, os.path.basename(vpu_dir))
        namelist_dir = os.path.join(namelist_dirs, os.path.basename(vpu_dir))
        output_dir = os.path.join(output_dirs, os.path.basename(vpu_dir))

        rapid_namelist_from_directories(vpu_directory=vpu_dir,
                                        inflows_directory=inflow_dir,
                                        namelists_directory=namelist_dir,
                                        outputs_directory=output_dir,
                                        datesubdir=datesubdir, )

    if dockerpaths and base_dir != '/mnt':
        for file in glob.glob(os.path.join(namelist_dirs, '*', 'namelist*')):
            with open(file, 'r') as f:
                text = f.read().replace(base_dir, '/mnt')
            with open(file, 'w') as f:
                f.write(text)
