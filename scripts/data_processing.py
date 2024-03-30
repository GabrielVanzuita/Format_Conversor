import csv 
import json

class Data: 
    
    def __init__(self, data):
        self.data = data
        self.name_columns = self._get_columns()
        self.asset_length = self._size_lines()

    def _size_lines(self):
        return len(self.data)

    def _get_columns(self):
        return list(self.data[0].keys())

    @staticmethod
    def _reading_csv(path):
        with open(path, 'r') as file:
            data_csv = []
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                data_csv.append(row)
        return data_csv        
        
    @staticmethod
    def _reading_json(path):
        with open(path, 'r') as file:
            data_json = json.load(file)
        return data_json 

    @classmethod
    def reading_data(cls, path, type):
        data = []

        if type == 'csv':
            data = cls._reading_csv(path)
        elif type == 'json':
            data = cls._reading_json(path)

        return data
    
    def rename_columns(self, key_mapping):
        new_data_csv = []
        for old_dict in self.data:
            temporary_dict = {}
            for key, object in old_dict.items():
                temporary_dict[key_mapping.get(key)] =  object
            new_data_csv.append(temporary_dict)
    
        self.data = new_data_csv
    
    @staticmethod
    def join(dataA, dataB):
        combined_list = []
        combined_list.extend(dataA)
        combined_list.extend(dataB)
        return combined_list

    def _transforming_table(self):
        combined_data = [self.name_columns]

        for row in self.data:
            linha = []
            for coluna in self.name_columns:
                linha.append(row.get(coluna, 'Indisponível'))
            combined_data.append(linha)

        return combined_data
    
    def saving_data(self, path):
        combined_data = self._transforming_table()

        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(combined_data)