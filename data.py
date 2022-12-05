import os
import pandas as pd
import csv

DATA_DIR = './data'
NUM_ROWS_PER_SPORT = 15000

users = []
events = None
bets = []
trades = []

def get_users():
    global users
    filename = os.path.join(DATA_DIR, 'user.csv')
    with open(filename, mode='r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        users = list(reader)
    users.pop(0)

def truncate_dataset():
    print('---------TRUNCATING DATASET---------')
    filename = os.path.join(DATA_DIR, 'betfair_140901.csv')
    df = pd.read_csv(filename)

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
    print(f':::::GENERATED {str(len(df_truncated))} LINES:::::')

def generate_categories():
    print('---------GENERATING CATEGORIES---------')
    filename = os.path.join(DATA_DIR, 'betfair.csv')
    df = pd.read_csv(filename)

    categories = df[['CATEGORY']].copy().drop_duplicates()

    filename = os.path.join(DATA_DIR, 'category.csv')
    categories.to_csv(filename, index=False, encoding='utf-8', sep=',')
    print(f':::::GENERATED {str(len(categories))} CATEGORIES:::::')

def generate_events():
    global events
    print('---------GENERATING EVENTS---------')
    filename = os.path.join(DATA_DIR, 'betfair.csv')
    df = pd.read_csv(filename)

    events = df[['CATEGORY', 'EVENT', 'START_TIME', 'END_TIME', 'ACTUAL_START_TIME']].copy().drop_duplicates()
    events['START_TIME'] = pd.to_datetime(df['START_TIME'], format='%d-%m-%Y %H:%M')
    events['END_TIME'] = pd.to_datetime(df['END_TIME'], format='%d-%m-%Y %H:%M:%S')
    events['ACTUAL_START_TIME'] = pd.to_datetime(df['ACTUAL_START_TIME'], format='%d-%m-%Y %H:%M:%S')

    function_dictionary = {'CATEGORY': pd.Series.mode, 'START_TIME': 'min', 'END_TIME': 'max', 'ACTUAL_START_TIME': 'min'}
    events = events.groupby('EVENT').agg(function_dictionary).reset_index()
    events.insert(0, 'EVENT_ID', events.index + 1)

    filename = os.path.join(DATA_DIR, 'event.csv')
    events.to_csv(filename, index=False, encoding='utf-8', sep=',')

    print(f':::::GENERATED {str(len(events))} EVENTS:::::')

def generate_markets():
    global events
    print('---------GENERATING MARKETS---------')
    filename = os.path.join(DATA_DIR, 'betfair.csv')
    df = pd.read_csv(filename)

    markets = df[['MARKET_ID', 'MARKET', 'EVENT']].copy().drop_duplicates()
    markets = markets.groupby(['EVENT', 'MARKET_ID']).sum().reset_index()
    markets['EVENT_ID'] = markets['EVENT'].map(events.set_index('EVENT')['EVENT_ID'])
    markets = markets.drop(['EVENT'], axis=1)

    filename = os.path.join(DATA_DIR, 'market.csv')
    markets.to_csv(filename, index=False, encoding='utf-8', sep=',')

    print(f':::::GENERATED {str(len(markets))} MARKETS:::::')

def generate_contracts():
    print('---------GENERATING CONTRACTS---------')
    filename = os.path.join(DATA_DIR, 'betfair.csv')
    df = pd.read_csv(filename)

    contracts = df[['CONTRACT_ID', 'CONTRACT', 'WINNER', 'MARKET_ID']].copy().drop_duplicates()
    contracts = contracts.groupby(['MARKET_ID', 'CONTRACT_ID']).agg({'CONTRACT': 'first', 'WINNER': 'first'}).reset_index()

    winner_sum = contracts.groupby('MARKET_ID').sum('WINNER')
    invalid_winner = winner_sum.loc[winner_sum['WINNER'] != 1].index.tolist()

    for market in invalid_winner:
        winner = contracts.loc[contracts['MARKET_ID'] == market].sort_values(by=['WINNER'], ascending=False).head(1)

        if winner['WINNER'].values[0] == 0:
            winner = contracts.loc[contracts['MARKET_ID'] == market].sample() # select a random winner
        else:
            winner = contracts.loc[(contracts['MARKET_ID'] == market) & (contracts['WINNER'] == 1)].sample() # randomly select one of the previous winners
        
        contracts.loc[contracts['MARKET_ID'] == market, 'WINNER'] = 0
        contracts.loc[(contracts['MARKET_ID'] == market) & (contracts['CONTRACT_ID'] == winner['CONTRACT_ID'].values[0]), 'WINNER'] = 1

    filename = os.path.join(DATA_DIR, 'contract.csv')
    contracts.to_csv(filename, index=False, encoding='utf-8', sep=',')

    print(f':::::GENERATED {str(len(contracts))} CONTRACTS:::::')

def generate_bets():
    print('---------GENERATING BETS/TRADES---------')
    filename = os.path.join(DATA_DIR, 'betfair.csv')
    df = pd.read_csv(filename)
    filename = os.path.join(DATA_DIR, 'user.csv')
    users = pd.read_csv(filename)
    valid_users = users.loc[(users['excluded'] == False) & (users['approved'] == True)]

    transactions = df[['MARKET_ID', 'CONTRACT_ID', 'ODDS', 'NUMBER_TRADES', 'TOTAL_VALUE']].copy().drop_duplicates()
    bets_dict, trades_dict = {}, {}
    bet_id, trade_id = 1, 1

    for _, trans in transactions.iterrows():
        trade_value = trans['TOTAL_VALUE'] / trans['NUMBER_TRADES']
        bet_value = trade_value / 2
        for _ in range(int(trans['NUMBER_TRADES'])):
            bet_users = valid_users.sample(n=2)
            back_user, lay_user = bet_users.iloc[0]['id'], bet_users.iloc[1]['id']
            bets_dict[bet_id] = [bet_id, int(trans['MARKET_ID']), int(trans['CONTRACT_ID']), trans['ODDS'], bet_value, back_user, 'BACK']
            bets_dict[bet_id + 1] = [bet_id + 1, int(trans['MARKET_ID']), int(trans['CONTRACT_ID']), trans['ODDS'], bet_value, lay_user, 'LAY']
            trades_dict[trade_id] = [bet_id, bet_id + 1, trans['ODDS'], trade_value]
            bet_id += 2
            trade_id += 1

    bets = pd.DataFrame.from_dict(bets_dict, orient='index', columns=['BET_ID', 'MARKET_ID', 'CONTRACT_ID', 'ODD', 'VALUE', 'USER_ID', 'TYPE'])
    trades = pd.DataFrame.from_dict(trades_dict, orient='index', columns=['BACK_BET', 'LAY_BET', 'ODD', 'VALUE'])

    filename = os.path.join(DATA_DIR, 'bet.csv')
    bets.to_csv(filename, index=False, encoding='utf-8', sep=',')
    filename = os.path.join(DATA_DIR, 'trade.csv')
    trades.to_csv(filename, index=False, encoding='utf-8', sep=',')

    print(f':::::GENERATED {str(len(bets))} BETS:::::')
    print(f':::::GENERATED {str(len(trades))} TRADES:::::')

def main():
    get_users()
    truncate_dataset()
    generate_categories()
    generate_events()
    generate_markets()
    generate_contracts()
    generate_bets()

if __name__ == '__main__':
    main()
