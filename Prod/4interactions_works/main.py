import numpy as np
from joblib import load
import sys
import json
import pandas as pd 
import signal_processing as sp
from tsfresh.utilities.dataframe_functions import impute
from tsfresh import extract_features
import matplotlib.pyplot as plt
#from keras.models import load_model
import collections

# Getting data
with open('features1.pkl', 'rb') as f:
    features = load(f)
# scaler = load('standard_scaler.pkl')

model_rf = load('baselineforest1.pkl')
std_window = 32
exp_alpha = 0.02
signal_length = 4000
k=0
indent = 500 # отступ для от порога 
treshold_f_ind = 100 # порог, по которому оценивается индекс, с которого начинать запись,

def Classificator(signal):
    signal = sp.moving_std_numpy(signal, std_window)[1:]
    signal = sp.exp_smooth_np(signal, alpha=exp_alpha)
    signal = sp.MinMax(signal)
  
    
    temp = pd.DataFrame({0: signal,
                         1: np.array([0]).repeat(len(signal))})
    exctr_features = extract_features(temp,
                                       column_id=1, 
                                       impute_function=impute,
                                       n_jobs=4,
                                       disable_progressbar=True,
                                       default_fc_parameters = features)
    

    #prob_nn = model_nn.predict(scaler.transform(exctr_features))   #for ann
    prob_rf = model_rf.predict_proba(exctr_features) # for RandomForest
    rf_predictions = {'Hit': prob_rf[0,0], 'Saw': prob_rf[0,1], 'Snack': prob_rf[0,2], 'Stairs': prob_rf[0,3]}

  
    json.dumps(rf_predictions)
    #print(ann_predictions)
    
 #   now = datetime.now()
 #   current_time = now.strftime("%H_%M_%S")
 #   np.save(f'./recordings/data_{current_time}', signal)
 #   answ = {'Bottle': prob[0,0], 'Saw': prob[0,1], 'Stairs': prob[0,2], 'Hit':prob[0,3], 'Snack':prob[0,4]}	
 #   with open(f'./recordings/data_answ_{current_time}.json', 'w') as f:
 #       json.dump(answ, f )
    return str(rf_predictions)

#Timur's detector
def Detector(signal, threshold_det):
	signals_std = np.std(signal)
	return signals_std > threshold_det
	
def first_index(first_second, indent,  threshold, std_wdw): 
    index_line = np.arange(len(first_second))
    std = sp.moving_std_numpy(first_second, std_wdw)[1:]
    index_line = np.arange(len(std))
    ind = index_line[std >= threshold]
    if len(ind) == 0: 
        print('High threshold, set a lower value')
        out = 0
    else:
        out = ind[0] 

    out -= indent 
    if out < 0: 
        out = 0
    return out    


if __name__ == '__main__':

    flag_previous_det = 0
    action_list = []
    while True:
        data=sys.stdin.buffer.read(80000)
        arr=np.frombuffer(data,dtype=np.double)
        arr = sp.central_chl(arr)
        if Detector(arr,threshold_det=150):
            action_list = np.concatenate((action_list,arr))
            #sys.stdout.write('alarm')
            if flag_previous_det == 0:
                flag_previous_det = 1
                f_index = first_index(arr, indent, treshold_f_ind, std_window)
                classificator_signal = arr[f_index:]
            else:
                classificator_signal = np.concatenate((classificator_signal, arr))
                if len(classificator_signal) >= signal_length:
                    classificator_signal = classificator_signal[:signal_length]
                    strClassify = Classificator(classificator_signal)
                    sys.stdout.write(strClassify)
                    flag_previous_det = 0
        else:
            if flag_previous_det == 1: 
                #sys.stdout.write('data collection')
                classificator_signal = np.concatenate((classificator_signal, arr))
                #print(len(classificator_signal))
                if len(classificator_signal) >= signal_length:
                    classificator_signal = classificator_signal[:signal_length]
                    strClassify = Classificator(classificator_signal)
                    sys.stdout.write(strClassify)
                    flag_previous_det = 0

                sys.stdout.flush()
            else:
                #sys.stdout.write('no')
                flag_previous_det = 0
                action_list = []
                sys.stdout.flush()

            sys.stdout.flush()
