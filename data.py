import os
import pandas as pd
import csv
from faker import Faker
import random

DATA_DIR = './data'
NUM_ROWS_PER_SPORT = 100000

users = []
languages = [('pt_PT', 150), ('es_ES', 115), ('fr_FR', 100), ('en_GB', 75), ('it_IT', 125), ('nl_NL', 69), ('de_DE', 95), ('da_DK', 86), ('sk_SK', 35), ('sv_SE', 50)]

def with_probability(probability):
    return random.random() < probability

# generate fake data and store into a CSV file
def generate_users():
    user_fields = ['id', 'name', 'address', 'date', 'phone', 'email', 'excluded', 'approved', 'balance', 'countryId']
    id = 1

    for lang in languages:
        fake = Faker(lang[0])
        
        # records = random.randint(75, 125)
        
        for _ in range(lang[1]):
            user = [id, fake.name(), fake.address().replace('\n', " "), fake.date_between(start_date='-90y', end_date='-18y'), str(fake.phone_number()), str(fake.email()), with_probability(0.08), with_probability(0.8), fake.random_number(digits=6),fake.current_country_code()]
            users.append(user)
            id += 1

    filename = os.path.join(DATA_DIR, 'user.csv')
    with open(filename, mode='w+', newline='', encoding='utf-8') as f:
        file_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        file_writer.writerow(user_fields)
        file_writer.writerows(users)


def generate_countries():
    countries = []
    country_fields = ['id', 'name']
    
    for lang in languages:
        fake = Faker(lang[0])
        country = [fake.current_country_code(), fake.current_country()]
        countries.append(country)

    filename = os.path.join(DATA_DIR, 'country.csv')
    with open(filename, mode='w+', newline='', encoding='utf-8') as f:
        file_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        file_writer.writerow(country_fields)
        file_writer.writerows(countries)

def generate_documents():
    documents = []
    fake = Faker()
    approved = True
    document_fields = ['id', 'type', 'file', 'approved', 'userId']
    for user in users:

        if user[7]:
            approved = True
        else:
            approved = False

        ext = random.choice(['pdf', 'png', 'jpeg'])

        document = [fake.random_number(digits=5), random.choice(['Driver License', 'Identification Card', 'Bill']), fake.file_name(extension=ext), approved, user[0]]
        documents.append(document)
        # sera necessario adicionar id do doc ao user?

    filename = os.path.join(DATA_DIR, 'document.csv')
    with open(filename, mode='w+', newline='', encoding='utf-8') as f:
        file_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        file_writer.writerow(document_fields)
        file_writer.writerows(documents)

def truncate_dataset():
    df = pd.read_csv('./data/betfair_140901.csv')

    df_portuguese_soccer = df.loc[df['FULL_DESCRIPTION'].str.contains('Portuguese Soccer', na=False)]
    df_portuguese_soccer = df_portuguese_soccer.replace('Sp Lisbon', 'Sporting Clube de Portugal', regex=True) # fixing Betfair's ignorance
    df_other_soccer = df.loc[df['SPORTS_ID'] == 1 & ~df['FULL_DESCRIPTION'].str.contains('Portuguese Soccer', na=False)].head(NUM_ROWS_PER_SPORT - df_portuguese_soccer.shape[0])
    df_soccer = pd.concat([df_portuguese_soccer, df_other_soccer], ignore_index=True, sort=False)
    df_tennis = df.loc[df['SPORTS_ID'] == 2].head(NUM_ROWS_PER_SPORT)
    df_basket = df.loc[df['SPORTS_ID'] == 7522].head(NUM_ROWS_PER_SPORT)

    df_truncated = pd.concat([df_soccer, df_tennis, df_basket], ignore_index=True, sort=False)

    df_truncated = df_truncated.rename(columns={
        'SPORTS_ID':'CATEGORY', 'EVENT_ID':'MARKET_ID', 'SETTLED_DATE':'END_TIME',
        'FULL_DESCRIPTION':'EVENT', 'SCHEDULED_OFF':'START_TIME', 'EVENT':'MARKET',
        'DT ACTUAL_OFF':'ACTUAL_START_TIME', 'SELECTION_ID':'CONTRACT_ID', 'SELECTION':'CONTRACT',
        'NUMBER_BETS':'NUMBER_TRADES', 'VOLUME_MATCHED':'TOTAL_VALUE', 'LATEST_TAKEN':'LATEST_TRADE',
        'FIRST_TAKEN':'FIRST_TRADE', 'WIN_FLAG':'WINNER', 'IN_PLAY':'STATE'
    })
    df_truncated['CATEGORY'] = df_truncated['CATEGORY'].map({1:'soccer', 2:'tennis', 7522:'basket'})

    filename = os.path.join(DATA_DIR, 'betfair.csv')
    df_truncated.to_csv(filename, index=False, encoding='utf-8', sep=',')

def generate_bets():
    df = pd.read_csv('./data/betfair.csv')

    # create table Category
    categories = df[['CATEGORY']].copy().drop_duplicates()

    # create table Event
    # TODO: fix
    events = df[['CATEGORY', 'EVENT', 'START_TIME', 'END_TIME', 'ACTUAL_START_TIME']].copy().drop_duplicates()
    function_dictionary = {'START_TIME': 'min', 'END_TIME': 'max', 'ACTUAL_START_TIME': 'min'}
    events['START_TIME'] = pd.to_datetime(df['START_TIME'], format='%d-%m-%Y %H:%M')
    events['END_TIME'] = pd.to_datetime(df['END_TIME'], format='%d-%m-%Y %H:%M:%S')
    events['ACTUAL_START_TIME'] = pd.to_datetime(df['ACTUAL_START_TIME'], format='%d-%m-%Y %H:%M:%S')
    # events.groupby('EVENT', as_index=True).agg(function_dictionary)
    events = events.groupby("EVENT").aggregate(function_dictionary)
    # events.insert(0, "EVENT_ID", events.index + 1)

    for table, df in [('category', categories), ('event', events)]:
        filename = os.path.join(DATA_DIR, f'{table}.csv')
        df.to_csv(filename, index=False, encoding='utf-8', sep=',')

def main():
    generate_users()
    generate_countries()
    generate_documents()
    truncate_dataset()
    generate_bets()

if __name__ == "__main__":
    main()
    
    