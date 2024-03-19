from datetime import datetime

def convert_time_format(time_str):
    # Determine if it's AM or PM
    am_pm_indicator = time_str[-1].lower()
    am_pm_mapping = {'p': 'PM', 'a': 'AM'}
    am_pm_indicator = am_pm_mapping.get(am_pm_indicator, '')
    time_str_without_indicator = time_str.rstrip('apAP')

    # Parse the time string without the indicator
    datetime_object = datetime.strptime(time_str_without_indicator, "%I:%M")
    if am_pm_indicator:
        datetime_object = datetime_object.replace(hour=datetime_object.hour + 12 if am_pm_indicator == 'PM' else datetime_object.hour)

    time_24h_format = datetime_object.strftime("%H:%M:%S")
    
    return time_24h_format

def assign_teams(row):
    row['team'] = row['home'] if row['is_home'] == 1 else row['away']
    row['rival'] = row['away'] if row['is_home'] == 1 else row['home']

    row['pts_team'] = row['home_pts'] if row['is_home'] == 1 else row['away_pts']
    row['pts_rival'] = row['away_pts'] if row['is_home'] == 1 else row['home_pts']
    return row