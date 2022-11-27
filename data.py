import pandas as pd
import csv
from faker import Faker
import random

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

    with open('csv/user.csv', mode='w+', newline='') as f:
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

    with open('csv/country.csv', mode='w+', newline='') as f:
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

    with open('csv/document.csv', mode='w+', newline='') as f:
        file_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        file_writer.writerow(document_fields)
        file_writer.writerows(documents)


if __name__ == "__main__":
    generate_users()
    generate_countries()
    generate_documents()
