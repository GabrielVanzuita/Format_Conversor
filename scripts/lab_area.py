####################################################
                    #PREPARATION
####################################################
import csv 
import json
from data_processing import Data

file_path_B = 'Documents/pipeline_dados/data_raw/data_B.csv' 
file_path_A = 'Documents/pipeline_dados/data_raw/data_A.json'
path_processed_data = 'Documents/pipeline_dados/data_processed/combined_data.csv'

key_mapping = {
    'Nome do Item': 'Nome do Produto',
    'Classificação do Produto': 'Categoria do Produto',
    'Valor em Reais (R$)': 'Preço do Produto (R$)',
    'Quantidade em Estoque': 'Quantidade em Estoque',
    'Nome da Loja': 'Filial',
    'Data da Venda': 'Data da Venda'
}

####################################################
                    #READING
####################################################

data_A = Data.reading_data(file_path_A, 'json')  
data_B = Data.reading_data(file_path_B, 'csv')  

####################################################
                    #FUSION
####################################################

merged_data = Data.join(data_A, data_B)
#print(merged_data)

####################################################
                    #SAVING
####################################################


merged_data_instance = Data(merged_data)
merged_data_instance.saving_data(path_processed_data)