import json
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

class OfferParser:
    def __init__(self, config_file='./src/parse_config.txt'):
        # Пример строки с настройками. Сделано для универсализации парсинга
        # Настройка парсинга состоит из списка словарей. Каждый словарь отвечает за извлечение отдельного датасета
        # В каждом словаре обязательным является только наличие записи с ключом path - в нём задана очередность парсинга json объекта в виде списка
        #  - {'key':'defaultState'} такая контрукция нужна для прохода по уровню json объекта вида списка. при проходе ищется запись с ключом key и значением defaultState
        #  - строковые значения - это ключ словаря json объекта 
        # 'orientation':'long'/'wide' отвечает за представление данных датафрейма, в который преобразуется извлеченный json объект. По уумолчанию = wide, то есть без изменений
        # 'drop': ['publishTerms.', 'moderationInfo.'] 
        #  - перечень колонок датафрейма, которые нужно удалить. 
        #  - поддерживаются регулярные выражения. Можно указать начальные символы для удаения нескольких
        # parse_config = [{'orientation':'long', 'path': [{'key':'defaultState'}, 'value', 'offerData', 'offer'], 'drop': ['publishTerms.', 'moderationInfo.']},
        #     {'orientation':'wide', 'path': [{'key':'defaultState'}, 'value', 'offerData', 'priceChanges']},
        #     {'orientation':'long', 'path': [{'key':'defaultState'}, 'value', 'offerData', 'bti']},]
        with open(config_file, 'r', encoding='utf-8') as f:            
            self.parse_config = json.load(f)    

        self.parsed_data = dict()       
        Path('./data/processed/').mkdir(parents=True, exist_ok=True) 

    def parse_from_dir(self, dir='./data/process/', limit = 100):        
        for index, path in enumerate(Path(dir).rglob('*.json')):
            if index > limit:
                break
            with Path(path).open('r', encoding='utf-8') as f:            
                #print(os.path.basename(path))
                self.parse_content(json.load(f), os.path.basename(path).split('.')[0]) 
                os.rename(str(path), str(path).replace('process', 'processed'))

                
    def parse_content(self, content, item_id):
        for config_row in self.parse_config:    
            parsed_dict = content
            config_path = config_row.get('path') 
            config_norm = config_row.get('norm')    
            config_drop = config_row.get('drop') 
            config_orientation = config_row.get('orientation')    
            for path_item in config_path:        
                if type(path_item) is dict:            
                    key = next(iter(path_item))
                    value = path_item.get(key)                        
                    for row in parsed_dict:
                        if row.get(key) == value:
                            parsed_dict = row                    
                            break
                elif type(path_item) is str:
                    parsed_dict = parsed_dict.get(path_item)
            if parsed_dict:
                df = pd.json_normalize(parsed_dict)    
                
                if config_orientation == 'long':
                    # extract lists of dicts only in long mode
                    df_det = pd.DataFrame(columns=['prop', 'val'])#.reset_index()
                    if config_norm:                                                
                        for norm_item in config_norm:    
                            df_norm = df.get(norm_item)
                            if df_norm is not None:
                                df_norm = pd.json_normalize(df.get(norm_item).explode(norm_item))                            
                                df_norm = pd.melt(df_norm, var_name='prop', value_name="val", ignore_index=False)
                                df_norm['prop'] = norm_item + '.' + df_norm['prop'] + '_' + df_norm.index.astype(str)
                                #display(df_norm)                            
                                df_det = pd.concat([df_det, df_norm])
                                df.drop(norm_item, axis='columns', inplace=True)

                if config_drop:
                    for drop_item in config_drop:
                        df = df[df.columns.drop(list(df.filter(regex=drop_item)))]

                if config_orientation == 'long':
                    df = pd.concat([df_det, pd.melt(df, var_name='prop', value_name="val")])
                    
                df['id'] = int(item_id)
                
                saved_df = self.parsed_data.get(path_item)
                if saved_df is not None:
                    self.parsed_data[path_item] = pd.concat([self.parsed_data[path_item], df])
                else:
                    self.parsed_data[path_item] = df

    def dump_to_db(self, connection_string):
        engine = create_engine(connection_string)
        for table_name, df in self.parsed_data.items():            
            df.to_sql(table_name + '_temp', engine, if_exists='replace')

if __name__ == '__main__':  
    offers = OfferParser()    
    offers.parse_from_dir()            
    offers.dump_to_db('postgresql://postgres:nhfvjynfyf@localhost:5433/test')