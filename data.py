import os
import pandas as pd
import csv
from faker import Faker
import random

DATA_DIR = './data'
NUM_ROWS_PER_SPORT = 100000

users = []
languages = ['pt_PT', 'es_ES', 'fr_FR', 'en_US']

def with_probability(probability):
    return random.random() < probability

# generate fake data and store into a JSON file
def generate_users():
    user_fields = ['id', 'name', 'address', 'date', 'phone', 'email', 'excluded', 'approved', 'balance', 'countryId']

    for lang in languages:
        fake = Faker(lang)
        
        records = random.randint(75, 125)
        
        for _ in range(records):
            user = [fake.random_number(digits=5), fake.name(), fake.address().replace('\n', " "), fake.date(), str(fake.phone_number()), str(fake.email()), with_probability(0.08), with_probability(0.8), fake.random_number(digits=6),fake.current_country_code()]
            users.append(user)

    filename = os.path.join(DATA_DIR, 'user.csv')
    with open(filename, mode='w+', newline='') as f:
        file_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        file_writer.writerow(user_fields)
        file_writer.writerows(users)


def generate_countries():
    countries = []
    country_fields = ['id', 'name']
    
    for lang in languages:
        fake = Faker(lang)
        country = [fake.current_country_code(), fake.current_country()]
        countries.append(country)

    filename = os.path.join(DATA_DIR, 'country.csv')
    with open(filename, mode='w+', newline='') as f:
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
    with open(filename, mode='w+', newline='') as f:
        file_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        file_writer.writerow(document_fields)
        file_writer.writerows(documents)

def generate_bets():
    df = pd.read_csv('./data/betfair_140901.csv')

    df_portuguese_soccer = df.loc[df['FULL_DESCRIPTION'].str.contains('Portuguese Soccer', na=False)]
    df_portuguese_soccer = df_portuguese_soccer.replace('Sp Lisbon', 'Sporting Clube de Portugal', regex=True) # fixing Betfair's ignorance
    df_other_soccer = df.loc[df['SPORTS_ID'] == 1 & ~df['FULL_DESCRIPTION'].str.contains('Portuguese Soccer', na=False)].head(NUM_ROWS_PER_SPORT - df_portuguese_soccer.shape[0])
    df_soccer = pd.concat([df_portuguese_soccer, df_other_soccer], ignore_index=True, sort=False)
    df_tennis = df.loc[df['SPORTS_ID'] == 2].head(NUM_ROWS_PER_SPORT)
    df_basket = df.loc[df['SPORTS_ID'] == 7522].head(NUM_ROWS_PER_SPORT)

    df_truncated = pd.concat([df_soccer, df_tennis, df_basket], ignore_index=True, sort=False)

    filename = os.path.join(DATA_DIR, 'betfair.csv')
    df_truncated.to_csv(filename, index=False, encoding='utf-8', sep=';')

if __name__ == "__main__":
    generate_users()
    generate_countries()
    generate_documents()
    generate_bets()
