import argparse
from math import ceil


#TODO: this value should be auto updated with the correct implementation of WCT
#default_emod3d_coef=6860795
default_emod3d_coef=3.00097
default_ncore=120
default_wct_scale=1.5
default_round_len=2
#hyperthread
default_hyperthread=False



#get nx, ny,nz by dividing extended_* with hh
def est_cour_hours_emod3d(nx, ny, nz,dt, sim_duration, emod3d_coef=default_emod3d_coef, round_len=default_round_len):
    '''
    returns a float formate core hour
    '''
    total_area=int(nx)*int(ny)*int(nz)
    timesteps=float(sim_duration)/float(dt)
    estimated_hours= ((total_area*timesteps)*emod3d_coef)/(10**11)
    #estimated_hours = estimated_seconds/3600
    if round_len != 0:
        estimated_hours = round(estimated_hours, round_len)
    return estimated_hours

#TODO:move shared funciton to a shared lib file
def est_wct(core_hours, ncore, scale=default_wct_scale):
    if scale < 1:
        print "Warning!! scale is under 1, may cause under estimating WCT."
    scaled_estimation = core_hours*scale
    #using ceil to round up the wct, so it will use at least one hour
    time_per_cpu = ceil(float(scaled_estimation/int(ncore)))
    #final fail-prof
    if time_per_cpu <= 1.0:
        time_per_cpu = 1.0
    estimated_wct = '{0:02.0f}:{1:02.0f}:00'.format(*divmod(time_per_cpu * 60, 60))
    #please keep note that the value may exceed the limit of one job, which is typically 23:59:59
    #its the script that called this function to be responsible of doing thise check
    return estimated_wct

if __name__ == '__main__':
      
    parser = argparse.ArgumentParser()

    
    #get a absolute path for a specific params.py to import(exe)
