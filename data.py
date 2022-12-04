import os
import pandas as pd
import csv
import random

DATA_DIR = './data'
NUM_ROWS_PER_SPORT = 50000

users = []
events = None
contracts = []
bets = []
trades = []

def get_users():
    global users
    with open("./data/user.csv", mode='r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        users = list(reader)
    users.pop(0)

def truncate_dataset():
    print('---------TRUNCATING DATASET---------')
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
    print(':::::GENERATED ' + str(len(df_truncated)) + ' LINES:::::')

def generate_categories():
    print('---------GENERATING CATEGORIES---------')
    df = pd.read_csv('./data/betfair.csv')

    categories = df[['CATEGORY']].copy().drop_duplicates()

    filename = os.path.join(DATA_DIR, 'category.csv')
    categories.to_csv(filename, index=False, encoding='utf-8', sep=',')
    print(':::::GENERATED ' + str(len(categories)) + ' CATEGORIES:::::')

def generate_events():
    global events
    print('---------GENERATING EVENTS---------')
    df = pd.read_csv('./data/betfair.csv')

    events = df[['CATEGORY', 'EVENT', 'START_TIME', 'END_TIME', 'ACTUAL_START_TIME']].copy().drop_duplicates()
    events['START_TIME'] = pd.to_datetime(df['START_TIME'], format='%d-%m-%Y %H:%M')
    events['END_TIME'] = pd.to_datetime(df['END_TIME'], format='%d-%m-%Y %H:%M:%S')
    events['ACTUAL_START_TIME'] = pd.to_datetime(df['ACTUAL_START_TIME'], format='%d-%m-%Y %H:%M:%S')

    function_dictionary = {'CATEGORY': pd.Series.mode, 'START_TIME': 'min', 'END_TIME': 'max', 'ACTUAL_START_TIME': 'min'}
    events = events.groupby('EVENT').agg(function_dictionary).reset_index()
    events.insert(0, "EVENT_ID", events.index + 1)

    filename = os.path.join(DATA_DIR, 'event.csv')
    events.to_csv(filename, index=False, encoding='utf-8', sep=',')

    print(':::::GENERATED ' + str(len(events)) + ' EVENTS:::::')

def generate_markets():
    global events
    print('---------GENERATING MARKETS---------')
    df = pd.read_csv('./data/betfair.csv')

    markets = df[['MARKET_ID', 'MARKET', 'EVENT']].copy().drop_duplicates()
    markets = markets.groupby(['EVENT', 'MARKET_ID']).sum().reset_index()
    markets['EVENT_ID'] = markets['EVENT'].map(events.set_index('EVENT')['EVENT_ID'])
    
    markets = markets.drop(['EVENT'], axis=1)

    filename = os.path.join(DATA_DIR, 'market.csv')
    markets.to_csv(filename, index=False, encoding='utf-8', sep=',')

    print(':::::GENERATED ' + str(len(markets)) + ' MARKETS:::::')

def generate_contracts():
    print('---------GENERATING CONTRACTS---------')
    global contracts
    contracts_fields = ['id', 'market_id', 'name', 'winner']
    betfair = []

    with open("./data/betfair.csv", mode='r', newline='', encoding='utf-8') as fo:
        reader = csv.reader(fo)
        betfair = list(reader)

    filename = os.path.join(DATA_DIR, 'contract.csv')
    with open(filename, mode='w+', newline='', encoding='utf-8') as f:
        file_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        file_writer.writerow(contracts_fields)

    betfair.pop(0)

    for row in betfair:
        if row[7] not in contracts:
            contract = [row[7], row[1], row[8], random.randint(0,1)]
            contracts.append(row[7])

            with open(filename, mode='a', newline='', encoding='utf-8') as f:
                file_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                file_writer.writerow(contract)

    print(':::::GENERATED ' + str(len(contracts)) + ' CONTRACTS:::::')

def generate_bets():
    print('---------GENERATING BETS/TRADES---------')
    global bets
    global trades
    bet_fields = ['id', 'contract_id', 'user_id', 'value', 'odd']
    trade_fields = ['id', 'bet_buyer_id', 'bet_seller_id', 'user_buyer_id', 'user_seller_id', 'value', 'odd']
    id_bet = 1
    id_trade = 1

    betfair = []

    with open("./data/betfair.csv", mode='r', newline='', encoding='utf-8') as fo:
        reader = csv.reader(fo)
        betfair = list(reader)

    filename = os.path.join(DATA_DIR, 'bet.csv')
    with open(filename, mode='w+', newline='', encoding='utf-8') as f:
        file_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        file_writer.writerow(bet_fields)

    filename1 = os.path.join(DATA_DIR, 'trade.csv')
    with open(filename1, mode='w+', newline='', encoding='utf-8') as f:
        file_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        file_writer.writerow(trade_fields)

    betfair.pop(0)

    for row in betfair:
        id_contract = row[7]
        number_trades = int(row[10])
        number_bets = number_trades * 2
        trade_value = float(row[11]) / number_trades
        bet_value = trade_value / 2
        odd = float(row[9])
        count_trades = 0
        count_bets = 0

        while count_trades < number_trades:
            while count_bets < number_bets:
                user_buyer_id = 0
                user_seller_id = 0
                excluded = True
                while (user_buyer_id == user_seller_id) and excluded:
                    user_buyer_id = random.randint(1, 900)
                    user_seller_id = random.randint(1, 900)
                    if users[user_buyer_id - 1][6] == "False" and users[user_seller_id - 1][6] == "False":
                        excluded = False 
                bet_seller_id = id_bet
                bets.append(id_bet)
                id_bet += 1
                bet_buyer_id = id_bet
                bets.append(id_bet)
                id_bet += 1
                count_bets += 2

                with open(filename, mode='a', newline='', encoding='utf-8') as f:
                    file_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    file_writer.writerow([bet_seller_id, id_contract, user_seller_id, bet_value, odd])
                    file_writer.writerow([bet_buyer_id, id_contract, user_buyer_id, bet_value, odd])

                with open(filename1, mode='a', newline='') as f:
                        file_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        file_writer.writerow([id_trade, bet_buyer_id, bet_seller_id, user_buyer_id, user_seller_id, bet_value, odd])
                
                trades.append(id_trade)
                id_trade += 1
                count_trades += 1

    print(':::::GENERATED ' + str(len(bets)) + ' BETS:::::')
    print(':::::GENERATED ' + str(len(trades)) + ' TRADES:::::')

def main():
    get_users()
    truncate_dataset()
    generate_categories()
    generate_events()
    generate_markets()
    generate_contracts()
    generate_bets()

if __name__ == "__main__":
    main()
