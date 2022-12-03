import os
import csv
from faker import Faker
import random

DATA_DIR = './data'
users = []
languages = [('pt_PT', 150), ('es_ES', 115), ('fr_FR', 100), ('en_GB', 75), ('it_IT', 125), ('nl_NL', 69), ('de_DE', 95), ('da_DK', 86), ('sk_SK', 35), ('sv_SE', 50)]

def with_probability(probability):
    return random.random() < probability

# generate fake data and store into a CSV file
def generate_users():
    print('---------GENERATING USERS---------')
    global users
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
    
    print(':::::GENERATED ' + str(len(users)) + ' USERS:::::')


def generate_countries():
    print('---------GENERATING COUNTRIES---------')
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

    print(':::::GENERATED ' + str(len(countries)) + ' COUNTRIES:::::')

def generate_documents():
    print('---------GENERATING DOCUMENTS---------')
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

    print(':::::GENERATED ' + str(len(documents)) + ' DOCUMENTS:::::')

def main():
    generate_users()
    generate_countries()
    generate_documents()

if __name__ == "__main__":
    main()