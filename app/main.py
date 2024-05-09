from data_creator import DataCreator

PRINTS_PATH = './app/prints.json'
TAPS_PATH = './app/taps.json'
PAYS_PATH = './app/pays.csv'
PROCESS_WEEKS = 3

if __name__ == '__main__':
    data_creator = DataCreator(PRINTS_PATH, TAPS_PATH, PAYS_PATH, PROCESS_WEEKS)
    dataset = data_creator.get_output_dataset()
    dataset.to_csv('./app/output.csv', index=False)
    print(dataset)