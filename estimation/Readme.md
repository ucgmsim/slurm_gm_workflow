#### Notes
- All of the estimation code is written in python3, the only exception is the 
write_jsons.py script used for metadata collection

### Usage
Estimation is done using the functions inside estimate_WC.py, which load the 
pre-trained neural network and then run the estimation.  
  
A model has to either exist in the  default model 
location ./estimation/models/(LF/HF or BB)/model_xxx.h5 along with 
pickled StandardScaler (sklearn) scaler_xxx.pickle, 
or a model direction has to be specified manually

Estimation of wall clock is already included in the 
slurm script creation for simulation  
  
For once off estimation the *estimate_LF_WC_single*, *estimate_HF_WC_single* 
and *estimate_BB_WC_single* functions from estimate_WC.py can be used  
```python
import estimation.estimate_WC as wc
print(wc.estimate_LF_WC_single(1000, 1000, 100, 2500))
```

### Creating a pre-trained model
Building a pre-trained model consists of a two main steps, 
**collect and format the data** and then **training the neural network**

 
 

