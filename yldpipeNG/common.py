import os
from pathlib import Path
import platform
data_master = 'dummy'

is_windows = False
app = None

uname_info = platform.uname()
#print(uname_info)
if uname_info.system == 'Windows':
    is_windows = True
    home_dir = Path('C:/')
    project_dir = home_dir.joinpath('dev/infra_tools')
    data_in = home_dir.joinpath('dev/infra_data/data_in/')
    data_out = home_dir.joinpath('dev/infra_data/data_out/')
    data_master = home_dir.joinpath('dev/infra_tools/data_master/')
elif uname_info.system == 'Linux':
    if uname_info.node == 'flowpad':
        project_dir = Path(__file__).parent.parent
        data_dir = project_dir.parent.joinpath('yldpipe_data')
        data_in = data_dir.joinpath('data_in')
        data_out = data_dir.joinpath('data_out')
        data_master = project_dir.joinpath('data_master')
    else:
        project_dir = Path(__file__).parent.parent

if app:
    project_dir = Path(__file__).parent.parent
    data_dir = project_dir.parent.joinpath('yldpipe_data')
    data_in = data_dir.joinpath('data_in')
    data_out = data_dir.joinpath('data_out')
    data_master = project_dir.joinpath('data_master')

# print('project_dir: ', project_dir)
