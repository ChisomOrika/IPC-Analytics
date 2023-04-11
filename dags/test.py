def get_today_data(df):
    # convert 'created_at' column to datetime type
    df['created_at'] = pd.to_datetime(df['created_at'])
    # set the start date to 24th March 2023
    start_date = pd.to_datetime('2023-03-24')
    # get today's date
    today = date.today()
    # filter rows where 'created_at' column corresponds to dates between start_date and today
    today_df = df[(df['created_at'].dt.date >= start_date.date()) & (df['created_at'].dt.date <= today)]
    # return the filtered DataFrame
    return today_df

def get_today_data(df):
    if df is None:
        raise ValueError("Input DataFrame is None.")
    # convert 'timestamp' column to datetime type
    df['created_at'] = pd.to_datetime(df['created_at'])
    # get today's date
    today = date.today()
    # filter rows where 'timestamp' column corresponds to today's date
    today_df = df[df['created_at'].dt.date == today]
    # return the filtered DataFrame
    return 
