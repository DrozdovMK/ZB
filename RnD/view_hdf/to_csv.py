import h5py
import csv
import json
# Открываем HDF5 файл
hdf5_file_path = 'data/online_saver/cesis.hdf5'  # Путь к вашему HDF5 файлу
csv_file_path = 'data/online_saver/cesis.csv'  # Путь для сохранения выходного CSV файла

# Открытие HDF5 файла
with h5py.File(hdf5_file_path, 'r') as hdf5_file:
    # Открываем CSV файл для записи
    with open(csv_file_path, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        
        # Записываем заголовок CSV файла (имена атрибутов)
        csv_writer.writerow(['dataset_name', 'probabilities', 'date_time'])
        
        # Проходим по всем датасетам в HDF5 файле
        for dataset_name in hdf5_file.keys():
            dataset = hdf5_file[dataset_name]
            datetime, probs =  dataset.attrs.values()
            probs = dict(json.loads(json.loads(probs)))
            prediction_best, prob_best = max(probs.items(), key=lambda item: item[1])
            # Проходим по всем атрибутам датасета
            csv_writer.writerow([dataset_name, prediction_best, datetime])

print("Данные успешно записаны в", csv_file_path)