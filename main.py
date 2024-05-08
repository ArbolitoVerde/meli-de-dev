from data_creator import DataCreator

PRINTS_PATH = './prints.json'
TAPS_PATH = './taps.json'
PAYS_PATH = './pays.csv'
PROCESS_WEEKS = 3

if __name__ == '__main__':
    data_creator = DataCreator(PRINTS_PATH, TAPS_PATH, PAYS_PATH, PROCESS_WEEKS)
    dataset = data_creator.get_output_dataset()
    dataset.to_csv('output.csv', index=False) 