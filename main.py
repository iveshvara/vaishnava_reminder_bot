# Сделать дни рождения духовных учителей, как считать лунные др?
# Сделать запрос о дате рождения и программу уведомлять о до соседей. Расстояние по координатам.

from aiogram import Bot
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import Dispatcher, FSMContext
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from aiogram.utils import executor
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils.markdown import link

import requests
from geopy.geocoders import Yandex
from settings import TOKEN, YANDEX_API_KEY, GEONAMES_USERNAME
import sqlite3

import os
import asyncio
import aioschedule
from sys import platform
import datetime
import time
from calendar import monthrange
import xmltodict


class StatesInput(StatesGroup):
    message = State()
    name = State()
    date = State()


bot = Bot(TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())


connect = sqlite3.connect('base.db')
cursor = connect.cursor()


async def on_startup(_):
    connect.execute(
        'CREATE TABLE IF NOT EXISTS users(id_user INTEGER, first_name TEXT, last_name TEXT, '
            'username TEXT, language_code TEXT, latitude NUMERIC, longitude NUMERIC, '
            'address TEXT, country TEXT, area TEXT, city TEXT, uts INTEGER, uts_summer INTEGER, '
            'caturmasya_system TEXT, notification_time TEXT, reminder INTEGER, last_message_id INTEGER)')
    connect.execute(
        'CREATE TABLE IF NOT EXISTS translations(language_code TEXT, mark TEXT, text TEXT, link TEXT)')
    connect.execute(
        'CREATE TABLE IF NOT EXISTS calendars(id_user INTEGER, masa_name TEXT, gyear INTEGER, '
            'date TEXT, dayweekid INTEGER, dayweek TEXT, '
            'sunrise_time TEXT, tithi_name TEXT, tithi_elapse NUMERIC, tithi_index INTEGER, '
            'naksatra_name TEXT, naksatra_elapse NUMERIC, '
            'yoga TEXT, paksa_id TEXT, paksa_name TEXT, '
            'dst_offset INTEGER, '
            'arunodaya_time TEXT, arunodaya_tithi_name TEXT, '
            'noon_time TEXT, sunset_time TEXT, '
            'moon_rise TEXT, moon_set TEXT, '
            'parana_from TEXT, parana_to TEXT, parana_after TEXT, '
            'vriddhi_sd BLOB, event INTEGER)')
    connect.execute('CREATE TABLE IF NOT EXISTS festivals(id_user INTEGER, name TEXT, class INTEGER)')
    connect.execute('CREATE TABLE IF NOT EXISTS caturmasya(id_user INTEGER, day TEXT, month INTEGER, system TEXT)')
    connect.execute('CREATE TABLE IF NOT EXISTS gurus(name TEXT, date TEXT, masa_name TEXT, tithi_index INTEGER)')

    connect.commit()

    asyncio.create_task(scheduler())


def shielding(text):
    text_result = ''
    forbidden_characters = '_*[]()~">#+-=|{}.!'
    for i in text:
        if i in forbidden_characters:
            text_result += '\\' + i
        else:
            text_result += i

    return text_result


def string_to_date(date_text='', time_text=''):
    if date_text == '' or time_text == 'N/A':
        return datetime.datetime.strptime('01-01-0001 00:00:00', '%d-%m-%Y %H:%M:%S')
    elif time_text == '':
        return datetime.datetime.strptime(date_text + ' 00:00:00', '%d %b %Y %H:%M:%S')
    else:
        return datetime.datetime.strptime(date_text + ' ' + time_text, '%d %b %Y %H:%M:%S')


async def translate(language_code, mark, need_shielding=True):
    cursor.execute(f'SELECT text, link FROM translations WHERE language_code = "{language_code}" AND mark = "{mark}"')
    result = cursor.fetchone()
    if result is None:
        if need_shielding:
            return shielding(mark)
        else:
            return mark
    else:
        text = result[0]
        link_text = result[1]

        if need_shielding:
            text = shielding(text)

        if link_text is not None and len(link_text) > 0:
            text = link(text, link_text) #[hyperlink](https://google.com)

        return text


async def translate_data(language_code, data, format_date):
    date_answer = ''
    data_text = data.strftime(format_date)
    data_list = data_text.split(' ')
    for i in data_list:
        if i.isalpha():
            date_answer += ' ' + await translate(language_code, i)
        else:
            date_answer += ' ' + i

    return date_answer.strip()


def get_moon_icon(tithi_index):
    #'🌕🌖🌗🌘🌑🌒🌓🌔'
    if tithi_index > 29 or tithi_index == 1:
        return '🌕'
    elif 2 <= tithi_index < 7:
        return '🌖'
    elif 7 <= tithi_index < 10:
        return '🌗'
    elif 10 <= tithi_index < 15:
        return '🌘'
    elif tithi_index == 15:
        return '🌑'
    elif 16 <= tithi_index < 22:
        return '🌒'
    elif 22 <= tithi_index < 25:
        return '🌓'
    elif 25 <= tithi_index < 30:
        return '🌔'


def add_months(source_date, months):
    month = source_date.month - 1 + months
    year = source_date.year + month // 12
    month = month % 12 + 1
    day = min(source_date.day, monthrange(year, month)[1])
    return datetime.date(year, month, day)


async def menu_get_location(message):
    language_code = message.from_user.language_code

    text = await translate(language_code, 'geolocation')
    send_location_title = await translate(language_code, 'Send a geo location')
    keyboard = ReplyKeyboardMarkup()
    keyboard.add(KeyboardButton(send_location_title, request_location=True))

    return text, keyboard


async def menu_settings(callback):
    id_user = callback.from_user.id

    list_str = callback.data.split()
    year = int(list_str[-3])
    month = int(list_str[-2])
    day = int(list_str[-1])

    cb_back_to_calendar = f'{year} {month} {day}'

    cursor.execute(f'SELECT * FROM users WHERE id_user = {id_user}')
    result = cursor.fetchone()
    language_code = result[4]
    latitude = result[5]
    longitude = result[6]
    address = result[7]
    country = result[8]
    area = result[9]
    city = result[10]
    uts = result[11]
    uts_summer = result[12]
    caturmasya_system = result[13]
    notification_time = result[14]
    reminder = result[15]

    text = '⚙`                    \.\.\.`'

    caturmasya_title = await translate(language_code, 'Caturmasya')
    caturmasya_value = await translate(language_code, caturmasya_system)
    reminder_text = await translate(language_code, 'reminder_' + str(reminder))
    notification_time_text = await translate(language_code, 'Notification Time')
    change_location = await translate(language_code, 'Change location')

    inline_kb = InlineKeyboardMarkup(row_width=1)
    inline_kb.add(InlineKeyboardButton(text='<<', callback_data='calendar ' + cb_back_to_calendar))
    inline_kb.row(InlineKeyboardButton(text='🇺🇸', callback_data='settings_answer language_code us ' + cb_back_to_calendar)
                  , InlineKeyboardButton(text='🇷🇺', callback_data='settings_answer language_code ru ' + cb_back_to_calendar)
                  # , InlineKeyboardButton(text='🇺🇦', callback_data='settings_answer language_code ua ' + cb_back_to_calendar)
                  )
    inline_kb.add(InlineKeyboardButton(text=caturmasya_title + ': ' + caturmasya_value, callback_data='settings caturmasya_system ' + cb_back_to_calendar))
    inline_kb.add(InlineKeyboardButton(text=notification_time_text + ': ' + notification_time, callback_data='settings notification_time ' + cb_back_to_calendar))
    inline_kb.add(InlineKeyboardButton(text=reminder_text, callback_data='settings reminder ' + cb_back_to_calendar))
    inline_kb.add(InlineKeyboardButton(text=change_location, callback_data='settings change_location ' + cb_back_to_calendar))

    return text, inline_kb


async def fill_calendar(id_user, latitude, longitude, uts, year):
    cursor.execute(f'SELECT language_code FROM users WHERE id_user = {id_user}')
    result = cursor.fetchone()
    language_code = result[0]

    start_date = datetime.datetime.strptime(f'{year}-{1}-{1}', '%Y-%m-%d')
    end_date = datetime.datetime.strptime(f'{year}-{12}-{31}', '%Y-%m-%d')

    lt = str(uts // 1) + 'E' + str(int(uts % 1 * 10)) + '0'  # часовой пояс в формате 3E00
    ty = year
    tm = 1
    td = 1
    tc = (end_date - start_date).days
    dst = '3x0x5x0x11x0x1x0' #'0x0x0x0x0x0x0x0' # этоти параметры определяюят как будет расчитан переход на летнее и зимнее время.

    files = []
    if platform == "linux" or platform == "linux2":
        start_date_linux = start_date
        for i in [100, 100, 100, tc-300]:
            end_date_linux = start_date_linux + datetime.timedelta(days=i)
            ty = start_date_linux.year
            tm = start_date_linux.month
            td = start_date_linux.day
            tc = i
            if tc < 100:
                tc += 1
            file_name = rf'{str(round(time.time() * 1000))}' + '.xml'
            cmd = fr'./gcal/lin/gcal-cgi "q=calendar&la={latitude}&lo={longitude}&lt={lt}' \
                  fr'&ty={ty}&tm={tm}&td={td}&tc={tc}&dst={dst}" > {file_name}'
            os.system(cmd)
            files.append((file_name, start_date_linux, end_date_linux))
            start_date_linux = end_date_linux

    elif platform == "win32" or platform == "win64":
        # gcal_cgi.exe "q=calendar&lc=Sevastopol&la=44.616604&lo=33.525369&ty=2019&tm=1&td=1&tc=4018&lt=3E00&dst=0x0x0x0x0x0x0x0" > name_file.xml
        tc += 1
        file_name = rf'{str(round(time.time() * 1000))}' + '.xml'
        cmd = fr'gcal\win\gcal_cgi.exe "q=calendar&la={latitude}&lo={longitude}&lt={lt}'\
              fr'&ty={ty}&tm={tm}&td={td}&tc={tc}&dst={dst}" > {file_name}'
        os.system(cmd)
        files.append((file_name, start_date, end_date))

    for i in files:
        file_name = i[0]
        start_date = i[1]
        end_date = i[2]

        xml_file = open(file_name, "r")
        xml_content = xml_file.read()
        xml_file.close()
        os.remove(file_name)
        xml_dict = xmltodict.parse(xml_content)

        masas = xml_dict['xml']['result']['masa']

        calendars = []
        caturmasya = []
        festivals = []

        for masa in masas:
            masa_name = await translate(language_code, masa['@name'])
            gyear = masa['@gyear']
            gyear_list = gyear.split(' ')
            masa_gyear = int(gyear_list[1])

            days = masa['day']
            if isinstance(days, dict):
                days = (days,)

            for day in days:
                date = string_to_date(day['@date'])
                sunrise = day['sunrise']
                sunrise_time = string_to_date(day['@date'], sunrise['@time'])
                tithi = sunrise['tithi']
                naksatra = sunrise['naksatra']
                yoga = sunrise['yoga']
                paksa = sunrise['paksa']

                arunodaya = day['arunodaya']
                arunodaya_time = string_to_date(day['@date'], arunodaya['@time'])

                noon_time = string_to_date(day['@date'], day['noon']['@time'])
                sunset_time = string_to_date(day['@date'], day['sunset']['@time'])
                moon_rise = string_to_date(day['@date'], day['moon']['@rise'])
                moon_set = string_to_date(day['@date'], day['moon']['@set'])

                parana_from = string_to_date()
                parana_to = parana_from
                parana_after = parana_from
                if 'parana' in day:
                    parana = day['parana']
                    if '@after' in parana:
                        parana_after = string_to_date(day['@date'], parana['@after'] + ':00')
                    else:
                        parana_from = string_to_date(day['@date'], parana['@from'] + ':00')
                        parana_to = string_to_date(day['@date'], parana['@to'] + ':00')

                if day['vriddhi']['@sd'] == 'yes':
                    vriddhi_sd = True
                else:
                    vriddhi_sd = False

                event = 10
                if 'caturmasya' in day:
                    caturmasya_list = day['caturmasya']
                    if isinstance(caturmasya_list, dict):
                        caturmasya_list = (caturmasya_list, )
                    for caturmasya_dict in caturmasya_list:
                        record = (id_user, date, caturmasya_dict['@day'], caturmasya_dict['@month'], caturmasya_dict['@system'])
                        caturmasya.append(record)
                        event = 6

                if 'festival' in day:
                    festival_list = day['festival']
                    if isinstance(festival_list, dict):
                        festival_list = (festival_list, )
                    for festival_dict in festival_list:
                        current_event = int(festival_dict['@class'])
                        if not event == 0:
                            if current_event == 0:
                                event = 0
                            else:
                                event = min(current_event, event)
                        record = (id_user, date, festival_dict['@name'], current_event)
                        festivals.append(record)

                tithi_name = await translate(language_code, tithi['@name'])
                naksatra_name = await translate(language_code, naksatra['@name'])
                yoga_name = await translate(language_code, yoga['@name'])
                paksa_name = await translate(language_code, paksa['@name'])
                arunodaya_tithi_name = await translate(language_code, arunodaya['tithi']['@name'])

                record = (id_user, masa_name, masa_gyear, date, int(day['@dayweekid']), day['@dayweek'],
                          sunrise_time, tithi_name, float(tithi['@elapse']), int(tithi['@index']),
                          naksatra_name, float(naksatra['@elapse']), yoga_name,
                          paksa['@id'], paksa_name, int(day['dst']['@offset']),
                          arunodaya_time, arunodaya_tithi_name,
                          noon_time, sunset_time, moon_rise, moon_set, parana_from, parana_to, parana_after, vriddhi_sd, event)

                calendars.append(record)

        if len(calendars) > 0 or len(caturmasya) > 0 or len(festivals) > 0:
            cursor.execute(f'DELETE FROM calendars WHERE id_user = {id_user} AND date BETWEEN "{start_date}" AND "{end_date}"')
            cursor.execute(f'DELETE FROM caturmasya WHERE id_user = {id_user} AND date BETWEEN "{start_date}" AND "{end_date}"')
            cursor.execute(f'DELETE FROM festivals WHERE id_user = {id_user} AND date BETWEEN "{start_date}" AND "{end_date}"')

            if len(calendars) > 0:
                cursor.executemany(
                    'INSERT INTO calendars (id_user, masa_name, gyear, date, dayweekid, dayweek, '
                    'sunrise_time, tithi_name, tithi_elapse, tithi_index, '
                    'naksatra_name, naksatra_elapse, yoga, paksa_id, '
                    'paksa_name, dst_offset, arunodaya_time, arunodaya_tithi_name, noon_time, sunset_time, moon_rise, '
                    'moon_set, parana_from, parana_to, parana_after, vriddhi_sd, event) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', calendars)

            if len(caturmasya) > 0:
                cursor.executemany('INSERT INTO caturmasya (id_user, date, day, month, system) VALUES (?, ?, ?, ?, ?)', caturmasya)

            if len(festivals) > 0:
                cursor.executemany('INSERT INTO festivals (id_user, date, name, class) VALUES (?, ?, ?, ?)', festivals)

            connect.commit()


async def display_calendar(id_user, year, month, day):
    #🎉
    cursor.execute(f'SELECT * FROM users WHERE id_user = {id_user}')
    users_tuple = cursor.fetchone()
    language_code = users_tuple[4]
    latitude = users_tuple[5]
    longitude = users_tuple[6]
    address = users_tuple[7]
    country = users_tuple[8]
    area = users_tuple[9]
    city = users_tuple[10]
    uts = users_tuple[11]
    uts_summer = users_tuple[12]
    caturmasya_system = users_tuple[13]

    selected_day_time = datetime.datetime(year, month, day)
    selected_day = selected_day_time.date()
    today = datetime.datetime.today().date()

    month_back = add_months(selected_day, -1)
    month_next = add_months(selected_day, 1)

    cb_back = f'calendar {month_back.year} {month_back.month} {1}'
    # cb_today = f'calendar {today.year} {today.month} {today.day}'
    cb_today = f'calendar now now now'
    cb_next = f'calendar {month_next.year} {month_next.month} {1}'

    cb_selected_day = f'{selected_day.year} {selected_day.month} {selected_day.day}'

    month_back_text = await translate(language_code, month_back.strftime('%B nominative case'))
    today_text = await translate(language_code, 'Today', False)
    month_next_text = await translate(language_code, month_next.strftime('%B nominative case'))

    inline_kb = InlineKeyboardMarkup(row_width=1)
    inline_kb.row(InlineKeyboardButton(text='⚙', callback_data='all_settings ' + cb_selected_day),
                  InlineKeyboardButton(text=month_back_text, callback_data=cb_back),
                  InlineKeyboardButton(text=today_text, callback_data=cb_today),
                  InlineKeyboardButton(text=month_next_text, callback_data=cb_next))

    number_of_days = monthrange(selected_day.year, selected_day.month)[1]
    start_date = datetime.datetime.strptime(f'{selected_day.year}-{selected_day.month}-{1}', '%Y-%m-%d')
    end_date = start_date + datetime.timedelta(days=number_of_days-1)

    cursor.execute(f'SELECT * FROM gurus WHERE NOT date is null')
    result = cursor.fetchall()
    gurus_list = []
    for i in result:
        guru_name = await translate(language_code, i[0])
        guru_date = datetime.datetime.strptime(i[1], '%Y-%m-%d %H:%M:%S')
        guru_masa_name = await translate(language_code, i[2])
        guru_tithi_index = int(i[3])
        gurus_list.append((guru_name, guru_date, guru_masa_name, guru_tithi_index))

    requisites = 'date, event, masa_name, tithi_index'
    cursor.execute(f'SELECT {requisites} FROM calendars WHERE id_user = {id_user} AND date BETWEEN "{start_date}" AND "{end_date}" ')
    result = cursor.fetchall()

    if len(result) == 0:
        await fill_calendar(id_user, latitude, longitude, uts, year)
        cursor.execute(f'SELECT {requisites} FROM calendars WHERE id_user = {id_user} AND date BETWEEN "{start_date}" AND "{end_date}" ')
        result = cursor.fetchall()

    array_week = []
    for i in result:
        date = datetime.datetime.strptime(i[0], '%Y-%m-%d %H:%M:%S').date()
        if date == selected_day == today or date == today:
            text_day = '🔴'
        elif date == selected_day:
            text_day = '⭕️'
        else:
            text_day = date.day

        event = i[1]
        event_icon = ''
        if event == 0:
            event_icon = '️‼️'
        elif event == -1:
            event_icon = '❗'
        elif event == 10:
            pass
        else:
            event_icon = '❕'

        if event_icon == '':
            masa_name = i[2]
            tithi_index = i[3]
            for ii in gurus_list:
                if ii[2] == masa_name and ii[3] == tithi_index:
                    event_icon = '❕'

        text_button = f'{text_day}{event_icon}'

        if len(array_week) == 0:
            week_day = date.weekday()
            for ii in range(week_day):
                array_week.append(InlineKeyboardButton(text='-', callback_data='-'))
        elif len(array_week) == 7:
            inline_kb.row(*array_week)
            array_week = []

        array_week.append(InlineKeyboardButton(text=text_button, callback_data=f'calendar {date.year} {date.month} {date.day}'))

    for ii in range(len(array_week), 7, 1):
        array_week.append(InlineKeyboardButton(text='-', callback_data='-'))

    if not len(array_week) == 0:
        inline_kb.row(*array_week)

    event_text = ''

    start_date = datetime.datetime(year, 1, 1)
    end_date = datetime.datetime(year, 12, 31)

    cursor.execute(f'''SELECT caturmasya1.date AS start_day, caturmasya2.date, caturmasya1.month AS end_day 
                    FROM caturmasya AS caturmasya1 LEFT JOIN caturmasya AS caturmasya2 
                   ON caturmasya1.id_user = caturmasya2.id_user AND caturmasya1.month = caturmasya2.month 
                   AND caturmasya1.system = caturmasya2.system 
                   WHERE caturmasya1.id_user = {id_user} AND caturmasya1.system = "{caturmasya_system}"
                   AND caturmasya1.day = "first" AND caturmasya2.day = "last" 
                   AND caturmasya1.date BETWEEN "{start_date}" AND "{end_date}" 
                   AND caturmasya2.date BETWEEN "{start_date}" AND "{end_date}"''')
    result_caturmasya = cursor.fetchall()

    for caturmasya_line in result_caturmasya:
        start_date = datetime.datetime.strptime(caturmasya_line[0], '%Y-%m-%d %H:%M:%S')
        end_date = datetime.datetime.strptime(caturmasya_line[1], '%Y-%m-%d %H:%M:%S')

        caturmasya_text = ''
        if selected_day_time == start_date:
           caturmasya_text = await translate(language_code, 'Caturmasya_beginning_of_month', False)
        elif selected_day_time == end_date:
            caturmasya_text = await translate(language_code, 'Caturmasya_end_of_month', False)
        elif start_date < selected_day_time < end_date:
            caturmasya_text = await translate(language_code, 'Caturmasya_month', False)

        if not caturmasya_text == '':
            caturmasya_month = caturmasya_line[2]
            caturmasya_title = await translate(language_code, 'Caturmasya')
            kartika = ''

            if caturmasya_month == 4:
                caturmasya_icon = '🪔'
                kartika = await translate(language_code, 'Kartika')
                kartika = f'\ ({kartika}\)'
            else:
                caturmasya_icon = '📜'

            caturmasya_text = caturmasya_text.format(caturmasya_month, kartika,
                await translate_data(language_code, start_date, '%d %B'),
                await translate_data(language_code, end_date, '%d %B'))
            event_text += '\n' + caturmasya_icon + ' *' + caturmasya_title + '\:* `' + caturmasya_text + '`\n'

    cursor.execute(f'SELECT * FROM calendars WHERE id_user = {id_user} AND date = "{selected_day_time}"')
    result = cursor.fetchone()

    masa_name = result[1]
    gyear = result[2]
    date = result[3]
    dayweekid = result[4]
    dayweek = result[5]
    sunrise_time = result[6]
    tithi_name = result[7]
    tithi_elapse = result[8]
    tithi_index = result[9]
    naksatra_name = result[10]
    naksatra_elapse = result[11]
    yoga = result[12]
    paksa_id = result[13]
    paksa_name = result[14]
    dst_offset = result[15]
    arunodaya_time = result[16]
    arunodaya_tithi_name = result[17]
    noon_time = result[18]
    sunset_time = result[19]
    moon_rise = result[20]
    moon_set = result[21]
    parana_from = result[22]
    parana_to = result[23]
    parana_after = result[24]
    vriddhi_sd = result[25]
    event = result[26]

    festivals_text = ''
    if not parana_from == '0001-01-01 00:00:00':
        end_the_fast = await translate(language_code, 'End the fast')
        festivals_text += f'\n{end_the_fast}: {parana_from[11:16]} \- {parana_to[11:16]}'
    elif not parana_after == '0001-01-01 00:00:00':
        end_the_fast = await translate(language_code, 'End the fast')
        after = await translate(language_code, 'after')
        festivals_text += f'\n{end_the_fast}: {after} {parana_after[11:16]}'

    event_icon = '🏵'
    if event < 10:
        if event == -1 and tithi_index in (11, 12, 26, 27):
            event_icon = ' 📿 '

        cursor.execute(f'SELECT name, CASE WHEN class = 0 THEN -2 ELSE class END As class FROM festivals WHERE id_user = {id_user} AND date = "{selected_day_time}"  ORDER BY class')
        result_festival = cursor.fetchall()

        for festival in result_festival:
            festivals_name = festival[0]
            if festivals_name[0] == '(':
                next_line = ' '
            else:
                next_line = '\n'
            festivals_text += next_line + await translate(language_code, festivals_name)

    if not festivals_text == '':
        festivals_text = f'\n{event_icon} *' + await translate(language_code, 'Events', False) + ':*' + festivals_text  # 🎉
        event_text += festivals_text + '\n'

    vyasapuji = ''
    for i in gurus_list:
        if i[2] == masa_name and i[3] == tithi_index:
            vyasapuji += '\n' + i[0]

    if not vyasapuji == '':
        vyasapuja_title = await translate(language_code, 'Vyasa-puja', True)
        vyasapuji = '\n🦚 *' + vyasapuja_title + ':* ' + vyasapuji

    event_text += vyasapuji

    # TODO сделать форматирование даты и времени в соответствии с кодом языка
    sun_rise = datetime.datetime.strptime(sunrise_time, '%Y-%m-%d %H:%M:%S')
    sun_set = datetime.datetime.strptime(sunset_time, '%Y-%m-%d %H:%M:%S')
    time_delta = (sun_set - sun_rise).total_seconds()
    duration = str(datetime.timedelta(seconds=time_delta))

    selected_day_text = await translate_data(language_code, selected_day, '%d %B %Y, %A')
    gaurabda = await translate(language_code, 'Gaurabda')

    address_text = ''
    if not city == '':
        address_text = ' — ' + city
    elif not area == '':
        address_text = ' — ' + area
    elif not country == '':
        address_text = ' — ' + country
    elif not address == '':
        address_text = ' — ' + address

    moon_icon = get_moon_icon(tithi_index)
    moon_rise = moon_rise[11:16]
    moon_set = moon_set[11:16]
    sunrise_time = sunrise_time[11:16]
    noon_time = noon_time[11:16]
    sunset_time = sunset_time[11:16]
    arunodaya_time = arunodaya_time[11:16]

    # text = '*' + selected_day_text + '*' + address_text + '\n' \
    #        + f'\n{moon_icon} *Лунный календарь\:* `Лунный день\: {tithi_index}\. \n' \
    #        + f'Тиртхи (название дня)\: {tithi_name}\.\n' \
    #        + f'Накшатра: {naksatra_name}\. Йога: {yoga}\.\n' \
    #        + f'Год \({gaurabda}\)\: {gyear}\. Маса (лунный месяц)\: {masa_name}\.`\n' \
    #        + f'\n🌙 *Луна\:* `восход\: {moon_rise[11:16]}\. Закат\: {moon_set[11:16]}\.`\n'\
    #        + f'\n☀️ *Солнце\:* `Восход\: {sunrise_time[11:16]}\. Закат\: {sunset_time[11:16]}\.\n'\
    #        + f'брахма мухурта\: {arunodaya_time[11:16]}\. Полдень\: {noon_time[11:16]}\.\n' \
    #        + f'продолжительность дня\: {duration}\.`\n' \
    #        + event_text

    main_template = await translate(language_code, 'main_template', False)
    text = main_template.format(selected_day_text, address_text,
                                moon_icon, tithi_index,
                                tithi_name,
                                naksatra_name, yoga,
                                gaurabda, gyear, masa_name,
                                moon_rise, moon_set,
                                sunrise_time, noon_time, sunset_time,
                                arunodaya_time,
                                duration,
                                event_text)
    return text, inline_kb


async def message_edit_text(text, inline_kb, callback=None, id_user = 0):
    if callback is None:
        if id_user == 0:
            return

        cursor.execute(f'SELECT last_message_id FROM users WHERE id_user = {id_user}')
        last_message_id = cursor.fetchone()[0]
        await bot.edit_message_text(text,
                                    chat_id=id_user,
                                    message_id=last_message_id,
                                    parse_mode='MarkdownV2',
                                    reply_markup=inline_kb,
                                    disable_web_page_preview=True)

    else:
        await callback.answer()
        if id_user == 0:
            if callback is not None:
                id_user = callback.message.chat.id

        try:
            msg = await callback.message.edit_text(text, parse_mode='MarkdownV2', reply_markup=inline_kb, disable_web_page_preview=True)
            cursor.execute(f'UPDATE users SET last_message_id = {msg.message_id} WHERE id_user = {id_user}')
            connect.commit()
        except:
            pass


async def scheduler():
    aioschedule.every().minute.do(run_reminder)
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)


async def run_reminder():
    cursor.execute(f'''SELECT
                        users.id_user, users.last_message_id,
                        datetime("now", users.uts || " hour", "start of day", "1 day") AS user_date,
                        calendars.parana_from, calendars.parana_to, calendars.parana_after,
                        strftime("%Y-%m-%d %H:%M:00", datetime("now", users.uts || " hour")) AS parana_date
                    FROM users LEFT JOIN calendars ON users.id_user = calendars.id_user 
                    WHERE users.reminder > 0 
                        AND calendars.event < CASE WHEN users.reminder = 2 THEN 10 ELSE 1 END 
                        AND (strftime("%H:%M", time("now", users.uts || " hour")) = strftime("%H:%M", time(users.notification_time))
                            AND calendars.date = user_date 
                            OR strftime("%Y-%m-%d %H:%M:00", calendars.parana_from) = parana_date 
                            OR strftime("%Y-%m-%d %H:%M:00", calendars.parana_after) = parana_date)''')

    result_tuple = cursor.fetchall()
    for i in result_tuple:
        id_user = i[0]
        last_message_id = i[1]
        user_date = i[2]
        date = datetime.datetime.strptime(user_date, '%Y-%m-%d %H:%M:%S')

        text, inline_kb = await display_calendar(id_user, date.year, date.month, date.day)
        await bot.delete_message(chat_id=id_user, message_id=last_message_id)
        msg = await bot.send_message(text=text, chat_id=id_user, parse_mode='MarkdownV2', reply_markup=inline_kb, disable_web_page_preview=True)

        cursor.execute(f'UPDATE users SET last_message_id = {msg.message_id} WHERE id_user = {id_user}')
        connect.commit()


@dp.message_handler(commands=['start'])
async def command_start(message: Message):
    id_user = message.from_user.id
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name
    if last_name is None:
        last_name = ''
    username = message.from_user.username
    if username is None:
        username = ''
    language_code = message.from_user.language_code

    cursor.execute(f'SELECT * FROM users WHERE id_user = {id_user}')
    result = cursor.fetchone()

    if result is None:
        new_user = True
        cursor.execute(
            'INSERT INTO users (id_user, first_name, last_name, username, language_code, latitude, longitude, address, country, area, city, uts, uts_summer, caturmasya_system, notification_time, reminder, last_message_id) '
            f'VALUES ({id_user}, "{first_name}", "{last_name}", "{username}", "{language_code}", 0, 0, "", "", "", "", 0, 0, "PURNIMA", "18:00", 1, 0)')
    else:
        new_user = result[5] == 0
        cursor.execute(f'UPDATE users SET first_name = "{first_name}", last_name = "{last_name}", '
                       f'username = "{username}", language_code = "{language_code}" WHERE id_user = {id_user}')
    connect.commit()

    if new_user:
        text, keyboard = await menu_get_location(message)
        text = await translate(language_code, 'greeting') + '\n' + text
    else:
        await message.answer('Ok', parse_mode='MarkdownV2', reply_markup=ReplyKeyboardRemove())

        today = datetime.datetime.today()
        text, keyboard = await display_calendar(id_user, today.year, today.month, today.day)

    msg = await message.answer(text, parse_mode='MarkdownV2', reply_markup=keyboard, disable_web_page_preview=True)
    cursor.execute(f'UPDATE users SET last_message_id = {msg.message_id} WHERE id_user = {id_user}')
    connect.commit()


@dp.message_handler(content_types=['location'])
async def handle_location(message: Message):
    id_user = message.from_user.id
    latitude = message.location.latitude
    longitude = message.location.longitude
    language_code = message.from_user.language_code

    cursor.execute(f'UPDATE users SET latitude = "{latitude}", longitude = "{longitude}" WHERE id_user = {id_user}')
    connect.commit()

    # geolocator = Nominatim(user_agent="vaishnava_reminder_bot")
    geolocator = Yandex(api_key=YANDEX_API_KEY)
    location = geolocator.reverse(f'{latitude}, {longitude}')

    address_path = location.raw['metaDataProperty']['GeocoderMetaData']['Address']
    address = address_path['formatted']
    country = ''
    area = ''
    city = ''
    components = address_path['Components']
    for i in components:
        if i['kind'] == 'country':
            country = i['name']
        elif i['kind'] == 'province':
            area = i['name']
        elif i['kind'] == 'area':
            area = i['name']
        elif i['kind'] == 'locality' and city == '':
            city = i['name']

    cursor.execute(f'UPDATE users SET address = "{address}", country = "{country}", area = "{area}", city = "{city}" WHERE id_user = {id_user}')
    connect.commit()

    if area in ("Севастополь", "Республика Крым"):
        uts = 3
        uts_summer = 3
    else:
        response_text = requests.get(f'http://api.geonames.org/timezoneJSON?formatted=true&lat={latitude}&lng={longitude}&username={GEONAMES_USERNAME}')  ## Make a request
        response = response_text.json()
        uts = response['rawOffset']
        uts_summer = response['dstOffset']

    cursor.execute(f'UPDATE users SET uts = "{uts}", uts_summer = "{uts_summer}" WHERE id_user = {id_user}')
    connect.commit()

    # TODO сделать определение текущего UTC
    uts_text = str(uts)
    if uts == 0:
        pass
    elif uts > 0:
        uts_text = '\+ ' + uts_text
    elif uts < 0:
        uts_text = '\- ' + uts_text
    uts_text += ' UTC'

    result = await translate(language_code, 'location confirmation', False)
    text = result.format(country, city, uts_text)

    await message.answer(text, parse_mode='MarkdownV2', reply_markup=ReplyKeyboardRemove(), disable_web_page_preview=True)

    today = datetime.datetime.today()
    text, inline_kb = await display_calendar(id_user, today.year, today.month, today.day)

    await message.answer(text, parse_mode='MarkdownV2', reply_markup=inline_kb, disable_web_page_preview=True)


@dp.callback_query_handler(lambda x: x.data and x.data.startswith('calendar '))
async def menu_back(callback: CallbackQuery):
    id_user = callback.from_user.id

    list_str = callback.data.split()
    year = list_str[1]
    month = list_str[2]
    day = list_str[3]

    if year == 'now':
        today = datetime.datetime.today().date()
        year = today.year
        month = today.month
        day = today.day
    else:
        year = int(year)
        month = int(month)
        day = int(day)

    text, inline_kb = await display_calendar(id_user, year, month, day)
    await message_edit_text(text, inline_kb, callback)


@dp.callback_query_handler(lambda x: x.data and x.data.startswith('all_settings '))
async def menu_settings_help(callback: CallbackQuery):

    text, inline_kb = await menu_settings(callback)
    await message_edit_text(text, inline_kb, callback)


@dp.callback_query_handler(lambda x: x.data and x.data.startswith('settings '))
async def menu_settings_help(callback: CallbackQuery):
    list_str = callback.data.split()
    command = list_str[1]
    year = int(list_str[2])
    month = int(list_str[3])
    day = int(list_str[4])

    id_user = callback.from_user.id
    language_code = callback.from_user.language_code
    cb_date = f'{year} {month} {day}'
    inline_kb = InlineKeyboardMarkup(row_width=1)
    text = ''

    if command == 'caturmasya_system':
        # text = await translate(language_code, 'Caturmasya') \
        #     + ' \(' + await translate(language_code, 'recommended') \
        #     + ' ' + await translate(language_code, 'PURNIMA') + '\)'
        #
        # inline_kb.add(InlineKeyboardButton(text=await translate(language_code, 'PURNIMA'), callback_data='settings_answer caturmasya_system PURNIMA ' + cb_date))
        # inline_kb.add(InlineKeyboardButton(text=await translate(language_code, 'PRATIPAT'), callback_data='settings_answer caturmasya_system PRATIPAT ' + cb_date))
        # inline_kb.add(InlineKeyboardButton(text=await translate(language_code, 'EKADASI'), callback_data='settings_answer caturmasya_system EKADASI ' + cb_date))

        cursor.execute(f'SELECT caturmasya_system FROM users WHERE id_user = {id_user}')
        result = cursor.fetchone()
        caturmasya_system = result[0]
        if caturmasya_system == 'PURNIMA':
            caturmasya_system = 'PRATIPAT'
        elif caturmasya_system == 'PRATIPAT':
            caturmasya_system = 'EKADASI'
        elif caturmasya_system == 'EKADASI':
            caturmasya_system = 'PURNIMA'

        cursor.execute(f'UPDATE users SET caturmasya_system = "{caturmasya_system}" WHERE id_user = {id_user}')
        connect.commit()

        text, inline_kb = await menu_settings(callback)

    elif command == 'notification_time':
        text = await translate(language_code, 'Notification Time')

        for i in range(12):
            time_text1 = str(i) + ':00'
            if i < 10:
                time_text1 = '0' + time_text1
            time_text2 = str(i + 12) + ':00'
            inline_kb.row(InlineKeyboardButton(text=time_text1, callback_data='settings_answer notification_time ' + time_text1 + ' ' + cb_date),
                          InlineKeyboardButton(text=time_text2, callback_data='settings_answer notification_time ' + time_text2 + ' ' + cb_date))


        # id_user = callback.from_user.id
        #
        # cursor.execute(f'SELECT notification_time FROM users WHERE id_user = {id_user}')
        # result = cursor.fetchone()
        # notification_time = int(result[0][:2])
        # notification_time += 2
        # if 0 <= notification_time < 10:
        #     notification_time_text = f'0{notification_time}:00'
        # elif 10 <= notification_time < 24:
        #     notification_time_text = f'{notification_time}:00'
        # else:
        #     notification_time_text = '00:00'
        #
        # cursor.execute(f'UPDATE users SET notification_time = "{notification_time_text}" WHERE id_user = {id_user}')
        # connect.commit()
        #
        # text, inline_kb = await menu_settings(callback)

    elif command == 'reminder':
        cursor.execute(f'SELECT reminder FROM users WHERE id_user = {id_user}')
        result = cursor.fetchone()
        reminder = result[0]
        if reminder == 0:
            reminder = 1
        elif reminder == 1:
            reminder = 2
        else:
            reminder = 0

        cursor.execute(f'UPDATE users SET reminder = {reminder} WHERE id_user = {id_user}')
        connect.commit()

        text, inline_kb = await menu_settings(callback)

    elif command == 'change_location':
        text, keyboard = await menu_get_location(callback)
        await callback.message.delete()
        await callback.message.answer(text, parse_mode='MarkdownV2', reply_markup=keyboard, disable_web_page_preview=True)

        return

    await message_edit_text(text, inline_kb, callback)


@dp.callback_query_handler(lambda x: x.data and x.data.startswith('settings_answer '))
async def menu_settings_help(callback: CallbackQuery):
    list_str = callback.data.split()
    command = list_str[1]
    answer = list_str[2]

    id_user = callback.from_user.id

    cursor.execute(f'UPDATE users SET {command} = "{answer}" WHERE id_user = {id_user}')
    connect.commit()

    text, inline_kb = await menu_settings(callback)
    await message_edit_text(text, inline_kb, callback)


@dp.message_handler(state=StatesInput.name)
async def input_start(message: Message, state: FSMContext):
    await state.update_data(name=message.text)

    data = await state.get_data()
    await data['message'].delete()
    await message.delete()

    message_answer = await message.answer("Enter the date in the format DD.MM.YYYY or DDMMYYYY")
    await state.update_data(message=message_answer)

    await StatesInput.next()


@dp.message_handler(state=StatesInput.date)
async def input_end(message: Message, state: FSMContext):
    await message.answer("`" + "result" + "`", parse_mode="MarkdownV2")


executor.start_polling(dp, skip_updates=False, on_startup=on_startup)
