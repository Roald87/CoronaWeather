import altair as alt
from altair import datum
from astral.geocoder import database, lookup
from astral.sun import daylight
from glob import glob   
from io import StringIO
import requests
import pandas as pd

YEAR_MAX_SUN_HOURS = 2019

def get_dutch_weather_data(startdate='19500101'):
    # Get data from KNMI api
    payload = {'vars': 'TN:TX:SQ', 'start': startdate}
    knmi_api_url = 'http://projects.knmi.nl/klimatologie/daggegevens/getdata_dag.cgi'
    r = requests.get(knmi_api_url, params=payload)
    data_text = r.text

    data = StringIO(data_text)
    knmi_data = pd.read_csv(data,
        sep=",",
        comment="#",
        header=None, 
        names="station date min_temp max_temp sun_h".split(),
        na_values='     ',
    )

    # Do some data clean-up, formatting and transformations
    knmi_data['date'] = pd.to_datetime(knmi_data["date"], format='%Y%m%d')
    
    knmi_data[[col for col in knmi_data if "temp" in col or 'sun' in col]] /= 10
    
    cols_to_look_for_nas = list(knmi_data)[2:]
    knmi_data.dropna(subset=cols_to_look_for_nas, how='all', inplace=True)
    
    knmi_data = knmi_data.groupby('date').mean()
    knmi_data.drop(columns='station', inplace=True)
    
    knmi_data['delta_temp'] = knmi_data['max_temp'] - knmi_data['min_temp']

    grouped = knmi_data.groupby(
        [knmi_data.index.month, knmi_data.index.day]
    )
    knmi_grouped_sun = grouped["sun_h"].max()
    max_theoretical_sun_hours = get_max_theoretical_sun_hours("Amsterdam", knmi_grouped_sun)

    knmi_data['max_sun_hours'] = max_theoretical_sun_hours['sunhours']

    return knmi_data

def get_swiss_weather_data(startdate='19500101'):
    swiss_weather_2020 = pd.read_csv(
        "https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/VQEA34.csv",
        sep=";",
        skiprows=1,
    )
    swiss_weather_2020['date'] = pd.to_datetime(swiss_weather_2020["time"], format='%Y%m%d')
    cols_to_drop = [
        'time',
        'gre000d0',
        'hto000d0',
        'nto000d0',
        'prestad0',
        'rre150d0',
        'tre200d0',
        'ure200d0'
    ]
    swiss_weather_2020.drop(columns=cols_to_drop, inplace=True)
    swiss_weather_2020 = swiss_weather_2020.groupby('date').mean()
    swiss_weather_2020.rename(
        {'sre000d0': 'sun_h', 'tre200dn': 'min_temp', 'tre200dx': 'max_temp'},
        axis='columns',
        inplace=True,
    )
    swiss_weather_2020['delta_temp'] = swiss_weather_2020['max_temp'] - swiss_weather_2020['min_temp']
    swiss_weather_2020['sun_h'] /= 60

    swiss_weather_old = pd.read_csv(
        'Average_weather_swiss_stations_below_1k.csv',
        index_col=0,
        parse_dates=True,
        infer_datetime_format=True,
    )

    swiss_weather = swiss_weather_old.append(swiss_weather_2020)

    grouped = swiss_weather.groupby(
        [swiss_weather.index.month, swiss_weather.index.day]
    )
    swiss_grouped_sun = grouped["sun_h"].max()
    max_theoretical_sun_hours = get_max_theoretical_sun_hours("Bern", swiss_grouped_sun)

    swiss_weather['max_sun_hours'] = max_theoretical_sun_hours['sunhours']

    year, month, day = startdate[:4], startdate[4:6], startdate[6:]
    _startdate = f"{year}-{month}-{day}"
    
    return swiss_weather[_startdate:]

def get_max_theoretical_sun_hours(city, max_real_sun_hours):
    city_data = lookup(city, database())
    start = f"{YEAR_MAX_SUN_HOURS}-01-01"
    end = f"{YEAR_MAX_SUN_HOURS}-12-31"
    dates = pd.date_range(start=start, end=end)
    daylight_hours = get_daylight_hours(city_data.observer, dates)

    grouped_daylight_hours = daylight_hours.groupby(
        [daylight_hours.index.month, daylight_hours.index.day]
    ).max()
    grouped_daylight_hours = pd.Series(grouped_daylight_hours['daylight_h'])
    
    _max_real_sun_hours = pd.Series(max_real_sun_hours)
    delta_sun_and_daylight = grouped_daylight_hours - _max_real_sun_hours
    min_delta = delta_sun_and_daylight.nsmallest(10).mean()
    
    daylight_hours['sunhours'] = daylight_hours['daylight_h'] - min_delta
    
    return daylight_hours

def get_daylight_hours(city, dates):
    """Get number of daylight hours for certain dated of a city"""
    duration_hours = []
    for date in dates:
        start, end = daylight(city, date=date)
        duration = end - start
        duration_hours.append(duration.total_seconds()/3600)
    
    daylight_hours = pd.DataFrame(
        [dates, duration_hours], 
        index=['date', 'daylight_h']
    ).T
    
    daylight_hours.set_index(['date'], inplace=True)
    
    return daylight_hours   

def get_historical_weather_data(path, save_to):
    """
    Data can be downloaded from: 
    https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/nbcn-tageswerte-1864-2018.zip 
    """
    fnames = glob(path + "*.csv")

    weather_stations_below_1k = {
        'ALT',
        'RAG',
        'BAS',
        'BER',
        'ELM',
        'GVE',
        'OTL',
        'LUG',
        'LUZ',
        'MER',
        'NEU',
        'PAY',
        'SIO',
        'STG',
        'SMA',
    }

    cols_to_drop = [
        'gre000d0',
        'hto000d0',
        'nto000d0',
        'prestad0',
        'rre150d0',
        'tre200d0',
        'ure200d0'
    ]

    first = True
    for fname in fnames:
        station = fname.split('_')[-2]
        if station not in weather_stations_below_1k:
            continue 
            
        data = pd.read_csv(fname, sep=";", na_values='-')
        data.drop(columns=cols_to_drop, inplace=True)
        
        if first:
            swiss_weather_stations = data
            first=False
        else:
            swiss_weather_stations = swiss_weather_stations.append(data, ignore_index=True)

    swiss_weather_stations['date'] = pd.to_datetime(swiss_weather_stations["date"], format='%Y%m%d')
    swiss_weather_stations = swiss_weather_stations.groupby('date').mean()
    swiss_weather_stations.rename(
        {'sre000d0': 'sun_h', 'tre200dn': 'min_temp', 'tre200dx': 'max_temp'},
        axis='columns',
        inplace=True,
    )
    swiss_weather_stations['delta_temp'] = swiss_weather_stations['max_temp'] - swiss_weather_stations['min_temp']
    swiss_weather_stations['sun_h'] /= 60

    swiss_weather_stations.to_csv(save_to)   

def calculate_rolling_mean(data, frame):
    base = alt.Chart(data.reset_index()
    ).transform_window(
        rolling_delta_temp=f'mean(delta_temp)',
        frame=frame
    ).transform_window(
        rolling_sun_h=f'mean(sun_h)',
        frame=frame
    ).transform_calculate(
        year='year(datum.date)'
    )
    
    return base

def plot_delta_temp(base):
    line_old, line_2020 = plot_weather_variable(
        base, variable="rolling_delta_temp", 
        y_axis_title="Temperature difference (°C)"
    )

    return line_old, line_2020


def plot_weather_variable(base, variable, y_axis_title):
    line_2020 = base.mark_line(size=3).encode(
        x='monthdate(date):T',
        y=f"{variable}:Q",
        color=alt.Color(
            'year:N', 
            title='', 
            scale=alt.Scale(
                domain=['2020', '2020'],
                range=['orange', 'orange']
                )
            )
    ).transform_filter(
        filter='datum.year == 2020'
    )

    line_old = base.mark_line(opacity=0.7).encode(
        alt.X('monthdate(date):T', title='Months'),
        alt.Y(f"{variable}:Q", title=y_axis_title, scale=alt.Scale(domain=(0, 18))),
        color=alt.Color('date:Q', timeUnit='year', scale=alt.Scale(scheme="lightgreyteal"), title="Year"),
    ).transform_filter(
        filter='datum.year < 2020'
    )
    
    return line_old, line_2020

def plot_max_sun_hours(base, city):
    max_sun = base.mark_line(color='Gray').encode(
        x='monthdate(date):T',
        y='max_sun_hours:Q',
        opacity=alt.Opacity(
            'year:O', 
            title='', 
            scale=alt.Scale(
                domain=[f'Max sun hours in {city}'] * 2,
                range=[1, 1]
            )
        )
    ).transform_filter(
        filter=f'datum.year == {YEAR_MAX_SUN_HOURS}'
    )

    line_old, line_2020 = plot_weather_variable(
        base, "rolling_sun_h", "Sun hours"
    )
    
    return max_sun, line_old, line_2020 
