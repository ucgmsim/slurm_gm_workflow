CODE = 'nz'

MODEL_VERSION = '1.64_NZ'
OUTPUT_DIR = 'Rapid_Model'
ORIGIN_LAT = '-43.6'
ORIGIN_LON = '172.3'
ORIGIN_ROT = '-10'
EXTENT_X = '140.0'
EXTENT_Y = '120.0'
EXTENT_ZMAX = '46.0'
EXTENT_ZMIN = '0'
EXTENT_Z_SPACING = '0.1'
EXTENT_LATLON_SPACING = '0.1'
MIN_VS = '0.5'
TOPO_TYPE= 'BULLDOZED'

HH = '0.100'
NX = '1400'
NY = '1200'
NZ = '460'
SIM_DURATION = '30.0'
SUFX = '_nz01-h0.100'

vel_mod_params_dir = os.path.join(global_root, 'VelocityModel/ModelParams')
GRIDFILE = os.path.join(vel_mod_params_dir, 'gridout%s'%SUFX) # containing the local (x,y,z) coordinates for this 3D run
MODEL_COORDS = os.path.join(vel_mod_params_dir, 'model_coords%s'%SUFX) # input for statgrid gen

