from configparser import ConfigParser
from random import randint, choice
from datetime import datetime, timedelta
import string
import globals

try:
    from faker import Faker
except ImportError:
    print("Install faker to start \"pip3 install faker\"")
    exit(101)

def execute_sql(cur: object, sql: str):
    try:
        cur.execute(sql)
        return cur
    except Exception as err:
        print("SQL Error: {}".format(sql))
        print(err)
        exit(150)

def is_object(cursor: object, sql: str) -> int:
    execute_sql(cursor, sql)
    return cursor.fetchone()[0]

def generate_date_in_range(config: ConfigParser) -> datetime:
    birth_start = config.get('settings', 'birth_start').split(',')
    birth_end = config.get('settings', 'birth_end').split(',')
    start = datetime(int(birth_start[0]), int(birth_start[1]), int(birth_start[2]))
    end = datetime(int(birth_end[0]), int(birth_end[1]), int(birth_end[2]))
    return start + timedelta(seconds=randint(0, int((end - start).total_seconds())))


def generate_citizen_id(config: ConfigParser, date: datetime, sex: int, f: Faker) -> str:
    if config.get('settings', 'language') == 'pl_PL':
        if date.date().year < 1999:
            pesel = date.date().strftime('%y%m%d')
        else:
            pesel = date.date().strftime('%y%m%d')[:2] \
                    + str(int(date.date().strftime('%y%m%d')[2:4]) + 20) + date.date().strftime('%y%m%d')[4:]
        pesel = pesel + ''.join(choice(string.digits) for _ in range(3))
        if sex == 0:
            pesel = pesel + choice(['0', '2', '4', '6', '8'])
        else:
            pesel = pesel + choice(['1', '3', '5', '7', '9'])
        vsum = 0
        position = 0
        for factor in [1, 3, 7, 9, 1, 3, 7, 9, 1, 3]:
            vsum += int(pesel[position:position + 1]) * factor
            position += 1
        sum_check = 10 - vsum % 10 if vsum % 10 != 0 else 0
        return pesel + str(sum_check)
    elif config.get('settings', 'language') == 'en_US':
        return f.ssn()
    else:
        return ''


def leading_zeros(value: str, length: int) -> str:
    value = ('0000000000' + str(value))
    return value[value.__len__() - length:]


def generate_driver_license(config: ConfigParser) -> str:
    if config.get('settings', 'language') == 'pl_PL':
        counties_codes = [[2, 26], [4, 19], [6, 20], [8, 12], [10, 21], [12, 19], [14, 38], [16, 11], [18, 21],
                          [20, 14], [22, 16], [24, 17], [26, 13], [28, 19], [30, 31], [32, 18]]
        cities_codes = [[2, 65], [4, 64], [6, 64], [8, 62], [10, 63], [12, 63], [14, 65], [16, 61], [18, 64], [20, 63],
                        [22, 64], [24, 78], [26, 61], [28, 62], [30, 64], [32, 63]]
        # generates new license format starting from 2002
        dl = leading_zeros(str(randint(1, 99999)), 5)
        dl += "/"
        dl += leading_zeros(str(randint(2, int(str(datetime.now().year)[2:]))), 2) + '/'
        if choice([0, 1]):
            county = choice(counties_codes)
            dl += leading_zeros(str(county[0]), 2)
            dl += leading_zeros(str(randint(1, county[1])), 2)
        else:
            county = choice(cities_codes)
            dl += leading_zeros(str(county[0]), 2)
            dl += leading_zeros(str(randint(61, county[1])), 2)
        return dl
    else:
        return ''


def generate_passport_id(config: ConfigParser) -> str:
    if config.get('settings', 'language') == 'pl_PL':
        #  wag_full = [7, 3, 9, 1, 7, 3, 1, 7, 3]
        wag_gen = [7, 3, 9, 7, 3, 1, 7, 3]
        symbols = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'U', 'V',
                   'W', 'X', 'Y', 'Z']
        d_values = []
        dl = ''
        for i in range(0, 2):
            value = choice(symbols)
            dl += value
            d_values.append(ord(value) - 55)
        value = leading_zeros(str(randint(0, 999999)), 6)
        for digit in list(value):
            d_values.append(int(digit))
        vsum = 0
        for i in range(0, 8):
            vsum += d_values[i] * wag_gen[i]
        dl += value[:1]
        dl += str(10 - vsum % 10) if vsum % 10 != 0 else '0'
        return dl + value[1:]
    else:
        return ''


def generate_citizen_document_id(config: ConfigParser) -> str:
    if config.get('settings', 'language'):
        #  wag_full = [7, 3, 1, 9, 7, 3, 1, 7, 3]
        wag_gen = [7, 3, 1, 7, 3, 1, 7, 3]
        symbols = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'U', 'V',
                   'W', 'X', 'Y', 'Z']
        d_values = []
        dl = ''
        for i in range(0, 3):
            value = choice(symbols)
            dl += value
            d_values.append(ord(value) - 55)
        value = leading_zeros(str(randint(0, 99999)), 5)
        for digit in list(value):
            d_values.append(int(digit))
        vsum = 0
        for i in range(0, 8):
            vsum += d_values[i] * wag_gen[i]
        return dl + str(vsum % 10) + value
    else:
        return ''


def remove_accents(input_text: str) -> str:
    strange = 'ŮôῡΒძěἊἦëĐᾇόἶἧзвŅῑἼźἓŉἐÿἈΌἢὶЁϋυŕŽŎŃğûλВὦėἜŤŨîᾪĝžἙâᾣÚκὔჯᾏᾢĠфĞὝŲŊŁČῐЙῤŌὭŏყἀхῦЧĎὍОуνἱῺèᾒῘᾘὨШūლἚύсÁóĒἍŷö' \
              'ὄЗὤἥბĔõὅῥŋБщἝξĢюᾫაπჟῸდΓÕűřἅгἰშΨńģὌΥÒᾬÏἴქὀῖὣᾙῶŠὟὁἵÖἕΕῨčᾈķЭτἻůᾕἫжΩᾶŇᾁἣჩαἄἹΖеУŹἃἠᾞåᾄГΠКíōĪὮϊὂᾱიżŦИὙἮὖÛĮ' \
              'ἳφᾖἋΎΰῩŚἷРῈĲἁéὃσňİΙῠΚĸὛΪᾝᾯψÄᾭêὠÀღЫĩĈμΆᾌἨÑἑïოĵÃŒŸζჭᾼőΣŻçųøΤΑËņĭῙŘАдὗპŰἤცᾓήἯΐÎეὊὼΘЖᾜὢĚἩħĂыῳὧďТΗἺĬὰὡὬὫÇ' \
              'ЩᾧñῢĻᾅÆßшδòÂчῌᾃΉᾑΦÍīМƒÜἒĴἿťᾴĶÊΊȘῃΟúχΔὋŴćŔῴῆЦЮΝΛῪŢὯнῬũãáἽĕᾗნᾳἆᾥйᾡὒსᾎĆрĀüСὕÅýფᾺῲšŵкἎἇὑЛვёἂΏθĘэᾋΧĉᾐĤὐὴι' \
              'ăąäὺÈФĺῇἘſგŜæῼῄĊἏØÉПяწДĿᾮἭĜХῂᾦωთĦлðὩზკίᾂᾆἪпἸиᾠώᾀŪāоÙἉἾρаđἌΞļÔβĖÝᾔĨНŀęᾤÓцЕĽŞὈÞუтΈέıàᾍἛśìŶŬȚĳῧῊᾟάεŖᾨᾉς' \
              'ΡმᾊᾸįᾚὥηᾛġÐὓłγľмþᾹἲἔбċῗჰხοἬŗŐἡὲῷῚΫŭᾩὸùᾷĹēრЯĄὉὪῒᾲΜᾰÌœĥტ'
    ascii_replacements = 'UoyBdeAieDaoiiZVNiIzeneyAOiiEyyrZONgulVoeETUiOgzEaoUkyjAoGFGYUNLCiIrOOoqaKyCDOOUniOeiII' \
                         'OSulEySAoEAyooZoibEoornBSEkGYOapzOdGOuraGisPngOYOOIikoioIoSYoiOeEYcAkEtIuiIZOaNaicaaIZE' \
                         'UZaiIaaGPKioIOioaizTIYIyUIifiAYyYSiREIaeosnIIyKkYIIOpAOeoAgYiCmAAINeiojAOYzcAoSZcuoTAEn' \
                         'iIRADypUitiiIiIeOoTZIoEIhAYoodTIIIaoOOCSonyKaAsSdoACIaIiFIiMfUeJItaKEISiOuxDOWcRoiTYNLY' \
                         'TONRuaaIeinaaoIoysACRAuSyAypAoswKAayLvEaOtEEAXciHyiiaaayEFliEsgSaOiCAOEPYtDKOIGKiootHLd' \
                         'OzkiaaIPIIooaUaOUAIrAdAKlObEYiINleoOTEKSOTuTEeiaAEsiYUTiyIIaeROAsRmAAiIoiIgDylglMtAieBc' \
                         'ihkoIrOieoIYuOouaKerYAOOiaMaIoht'
    translator = str.maketrans(strange, ascii_replacements)
    return input_text.translate(translator)


def generate_mail(config: ConfigParser, first: str, last: str, global_domains: [str]) -> str:
    first = remove_accents(first.lower())
    last = remove_accents(last.lower())
    if choice([True, False]):
        mail = first + '.' + last + '@'
    elif choice([True, False]):
        mail = first[:1] + last + '@'
    else:
        mail = first[:1] + '.' + last + '@'
    return mail + choice(global_domains)


def generate_wired_phone_number(lang: str) -> str:
    if lang == 'pl_PL':
        # ranges is an array of polish regional prefixes
        ranges = [[12, 18], [22, 25], [29, 29], [32, 34], [41, 44], [46, 46], [48, 48], [52, 52], [54, 56], [58, 59],
                  [61, 63], [65, 65], [67, 68], [71, 71], [74, 77], [81, 87], [89, 89], [91, 91], [94, 95]]
        zone = choice(ranges)
        return str(randint(zone[0], zone[1])) + leading_zeros(str(randint(0, 9999999)), 7)
    else:
        return ''


# generates mobile phone number, supports polish, empty value for other countries
def generate_mobile_phone_number(lang: str) -> str:
    if lang == 'pl_PL':
        # prefixes is an array of polish mobile carriers phone numbers
        prefixes = ['45', '50', '51', '53', '57', '60', '66', '69', '72', '73', '78', '79', '88']
        return choice(prefixes) + leading_zeros(str(randint(0, 9999999)), 7)
    else:
        return ''


# generates random phone number, for polish generates wired of mobile one, for other languages use faker
def generate_phone_number(config: ConfigParser, f: Faker) -> str:
    phone = generate_mobile_phone_number(config.get('settings', 'language')) if choice([True, False]) \
        else generate_wired_phone_number(config.get('settings', 'language'))
    if config.get('settings', 'language') == 'pl_PL':
        if choice([True, False]):
            if choice([True, False]):
                phone = generate_mobile_phone_number(config.get('settings', 'language'))
                phone = phone[:3] + '-' + phone[3:6] + '-' + phone[6:9]
            else:
                phone = generate_wired_phone_number(config.get('settings', 'language'))
                phone = '(' + phone[:2] + ')' + phone[2:5] + '-' + phone[5:7] + '-' + phone[7:9]
            return phone if choice([True, False]) else '+48' + phone
        else:
            return phone if choice([True, False]) else '+48' + phone
    else:
        return f.phone_number()


def generate_phone_number_by_locale(f: Faker) -> str:
    phone = generate_mobile_phone_number(f.locales[0]) if choice([True, False]) \
        else generate_wired_phone_number(f.locales[0])
    if f.locales[0] == 'pl_PL':
        if choice([True, False]):
            if choice([True, False]):
                phone = generate_mobile_phone_number(f.locales[0])
                phone = phone[:3] + '-' + phone[3:6] + '-' + phone[6:9]
            else:
                phone = generate_wired_phone_number(f.locales[0])
                phone = '(' + phone[:2] + ')' + phone[2:5] + '-' + phone[5:7] + '-' + phone[7:9]
            return phone if choice([True, False]) else '+48' + phone
        else:
            return phone if choice([True, False]) else '+48' + phone
    else:
        return f.phone_number()


def add_customer(config: ConfigParser, cur: object, global_domains: [str]):
    app_schema = globals.GAMES_SCHEMA
    fake = Faker(config.get('settings', 'language'))
    sex = randint(0, 2)
    birth_date = generate_date_in_range(config)
    first_name = fake.first_name() if sex == 1 else fake.first_name_female()
    last_name = fake.last_name() if sex == 1 else fake.last_name_female()

    if config.get('db', 'type') in ['postgres', 'oracle']:
        bd = "TO_DATE('{}','yyyy-mm-dd')".format(birth_date.date())
    elif config.get('db', 'type') == 'mysql':
        bd = "STR_TO_DATE('{}', '%Y-%m-%d')".format(birth_date.date())
    sql = "INSERT INTO {reference}.customers (" \
          "customer_fname," \
          "customer_lname," \
          "full_name," \
          "birthday," \
          "citizen_id," \
          "birth_place," \
          "street," \
          "flat_number," \
          "city," \
          "zipcode," \
          "driving_license," \
          "passport_id," \
          "citizen_doc_id," \
          "mail," \
          "phone)" \
          " VALUES('{fn}','{ln}','{full}',{bd},'{ci}','{bc}','{sn}','{fl}','{ct}','{zc}'," \
          "'{dl}','{ps}','{do}','{ma}'," \
          "'{pn}')".format(
                reference=app_schema,
                fn=first_name,
                ln=last_name,
                full=f"{first_name} {last_name}" if choice([True, False]) else f"{last_name} {first_name}",
                bd=bd,
                # if dbtype == 'Oracle' else "'" + str(bdate.date()) + "'",
                ci=generate_citizen_id(config, birth_date, sex, fake),
                bc=fake.city(),
                sn=fake.street_name(),
                fl=fake.numerify(text="###"),
                ct=fake.city(),
                zc=fake.postcode(),
                dl=generate_driver_license(config),
                ps=generate_passport_id(config),
                do=generate_citizen_document_id(config),
                ma=generate_mail(config, first_name, last_name, global_domains),
                pn=generate_phone_number(config, fake))
    del fake
    execute_sql(cur, sql)