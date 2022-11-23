import pandas as pd
from faker import Faker
import random

users = []
languages = ['pt_PT', 'es_ES', 'fr_FR', 'en_US']

def with_probability(probability):
    return random.random() < probability

# generate fake data and store into a JSON file
def generate_users(records):
    for lang in languages:
        fake = Faker(lang)
        
        
        for _ in range(records):
            user = {}
            user['id'] = fake.random_number(digits=5)
            user['name'] = fake.name()
            user['address'] = fake.address().replace('\n', " ")
            user['birth_date'] = fake.date()
            user['phone'] = str(fake.phone_number())
            user['email'] = str(fake.email())
            user['excluded'] = with_probability(0.05)
            user['approved'] = with_probability(0.9)
            user['balance'] = fake.random_number(digits=6)
            user['country'] = fake.current_country_code()
            users.append(user)

def generate_countries():
    df = pd.DataFrame()
    countries = []
    
    for lang in languages:
        country = {}
        fake = Faker(lang)
        country['id'] = fake.current_country_code()
        country['name'] = fake.current_country()
        countries.append(country)

    dfc = pd.DataFrame(countries)
    dfc.to_csv('country.csv', index=False)


if __name__ == "__main__":
    generate_users(100)

    generate_countries()

    df = pd.DataFrame(users)
    df.to_csv('user.csv', index=False)