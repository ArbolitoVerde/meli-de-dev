import json
import pandas as pd
from pandas import DataFrame


class DataCreator:

    SCHEMA = {
        'date': 'datetime64[ns]',
        'user_id': 'int32',
        'position': 'int32',
        'value_prop_print': 'str',
        'click_flag': 'int32',
        'views_last3_weeks': 'int32',
        'clicks_last3_weeks': 'int32',
        'pays_last3_weeks': 'int32',
        'import_pay_last3_weeks': 'float64'
    }

    def __init__(self, prints_path: str, taps_path: str, pays_path: str, process_weeks: int) -> None:
        self.__prints_path = prints_path
        self.__taps_path = taps_path
        self.__pays_path = pays_path
        self.__process_weeks = process_weeks


    def __read_file(self, file_path: str) -> DataFrame:
        """Hace lectura de los archivos inputs necesarios para calcular el dataset final.

        Args:
            file_path (str): localización del archivo a leer.

        Returns:
            DataFrame: se transforma el input ya sea json o csv a un dataframe.
        """

        data = []
        if file_path.endswith('.json'):
            try: 
                with open(f'{file_path}', 'r') as f:
                    for line in f:
                        data.append(json.loads(line))
                f.close()
                df = pd.json_normalize(data)
                df.rename(columns = {'day':'date', 'event_data.position':'position', 'event_data.value_prop':'value_prop'}, inplace = True)
                return df
            except FileNotFoundError as fnf_error:
                print(fnf_error)
            except:
                print("Something went wrong")
        elif file_path.endswith('.csv'):
            try: 
                df = pd.read_csv("pays.csv")
                df = df.groupby(['pay_date','user_id','value_prop'])['total'].sum()
                return df
            except FileNotFoundError as fnf_error:
                print(fnf_error)
            except:
                print("Something went wrong")
        else:
            print("Wrong extension file, only reading json or csv files")


    def __validate_key_fields(self, df: DataFrame) -> DataFrame:
        """Valida la existencia de duplicidad de la llave de los df de prints y taps, si existen, son eliminados
            retornando un dataset sin ellos.

        Args:
            df (DataFrame): Dataframe a validar.

        Returns:
            DataFrame: Dataframe sin duplicados.
        """
        duplicates = df.groupby(['date', 'user_id', 'position']).filter(lambda x: len(x) > 1).value_counts()
        if len(duplicates) > 0:
            return df.drop_duplicates(keep='last').reset_index()
        else:
            return df
        

    def __create_join_table(self, prints: DataFrame, taps: DataFrame, pays: DataFrame) -> DataFrame:
        """Genera el join de las tres tablas involucradas en el proceso.

        Args:
            prints (DataFrame): prints data
            taps (DataFrame): taps data
            pays (DataFrame): pays data

        Returns:
            DataFrame: datos cruzados en función del df input prints.
        """
        prints = self.__validate_key_fields(prints)
        taps = self.__validate_key_fields(taps)
        
        prints_taps = pd.merge(prints, taps, on=['date', 'user_id', 'position'], suffixes=('_print', '_tap'), how='left')
        joined_data = pd.merge(prints_taps, pays, left_on=['date','user_id','value_prop_print'], right_on=['pay_date','user_id','value_prop'], how='left')
        joined_data['click_flag'] = 1
        joined_data.loc[joined_data['value_prop_tap'].isnull(), 'click_flag'] = 0
        joined_data['date'] = pd.to_datetime(joined_data['date'])
        joined_data['year_week_id'] = joined_data['date'].dt.year.astype('int32') * 100 + \
                                        joined_data['date'].dt.isocalendar().week.astype('int32')
        joined_data['row_num_week'] = joined_data['year_week_id'].rank(method ='dense',ascending=True).astype('int32')
        return joined_data
    
    
    def __create_ouput_dataset(self, df: DataFrame, wk: int) -> DataFrame:
        """Genera la estructura con los datos solicitados como output

        Args:
            df (DataFrame): dataset sobre el cual se van a calcular los KPIs
            wk (int): cantidad de semanas móviles que se consideraran para calcular los KPIs

        Returns:
            DataFrame: df con la estructura y datos requeridos como output
        """
        max_week = df['row_num_week'].max() 
        early_weeks = df.query(f'row_num_week >= {max_week}-{wk} and row_num_week < {max_week}')
        agg_early_weeks = early_weeks.groupby(['user_id','value_prop_print']).aggregate({'value_prop_print':'count', 'click_flag':'sum','total':['count','sum']})
        agg_early_weeks = pd.DataFrame(agg_early_weeks.to_records())
        agg_early_weeks.columns = ['user_id', 'value_prop_print', f'views_last{wk}_weeks', f'clicks_last{wk}_weeks', f'pays_last{wk}_weeks', f'import_pay_last{wk}_weeks']

        last_week = df.query(f'row_num_week == {max_week}')
        tmp_output = pd.merge(last_week, agg_early_weeks, on=['user_id', 'value_prop_print'], how='left')
        output = tmp_output[['date', 'user_id', 'position', 'value_prop_print', 'click_flag', f'views_last{wk}_weeks', f'clicks_last{wk}_weeks', f'pays_last{wk}_weeks', f'import_pay_last{wk}_weeks']].fillna(0)
        output = output.astype(self.SCHEMA)
        return output


    def get_output_dataset(self) -> DataFrame:
        """Función que gestiona el llamado a los métodos privados para generar output.

        Returns:
            DataFrame: df con la estructura y datos requeridos como output
        """
        prints = self.__read_file(self.__prints_path)
        taps = self.__read_file(self.__taps_path)
        pays = self.__read_file(self.__pays_path)

        join_df = self.__create_join_table(prints, taps, pays)
        dataset = self.__create_ouput_dataset(join_df, self.__process_weeks)
        return dataset