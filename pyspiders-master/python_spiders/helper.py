import re
from datetime import datetime
import requests
from io import StringIO
from html.parser import HTMLParser


class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = StringIO()

    def handle_data(self, d):
        self.text.write(d)

    def get_data(self):
        return self.text.getvalue()


def strip_tags(html):
    """[This function is used to parse html to strings]
    Args:
        html [string]: [html]
    Returns:
        html[string]: [string]
    """
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def sq_feet_to_meters(_feet):
    """[This function is used to covert square feet as string to square meters]
    Args:
        sq_feet [string]: [sq_feet]
    Returns:
        sq_meters[int]: [sq_meters ]
    """
    _feet = int(_feet)
    return int(_feet / 10.764)


def get_price(price):
    """[This function is used to clean up the price string and the price value]
    Args:
        val [string]: [html]
    Returns:
        html[string]: [string]
    """
    value = int(float(extract_number_only(
        price, thousand_separator=',', scale_separator='.')))
    value_2 = int(float(extract_number_only(
        price, thousand_separator='.', scale_separator=',')))
    price = min(value, value_2)
    if price < 10:
        price = max(value, value_2)
    return price

    
def string_found(_word_list, _description):
    _description = _description.lower()
    for _word in _word_list:
        if re.search(r"\b" + re.escape(_word.lower()) + r"\b", _description):
            return True
    return False



def remove_white_spaces(input_string):
    """
    Removes continuous spaces in the input string
    :param input_string:
    """
    return re.sub(r'\s+', ' ', input_string).strip()


def remove_unicode_char(input_string):
    """
    This function takes string as an input, and return strings after removing unicode character
    """
    return (''.join([i if ord(i) < 128 else ' ' for i in input_string])).strip()


def extract_number_only(input_string, thousand_separator='.', scale_separator=','):
    """[This function is used to extract number from string, Common usecase is extract rent and square feet]
    Args:
        input_string ([type]): [should be string that have string + Numbers]
    Returns:
        [type]: [numbers from string ]
    """
    input_string = str(input_string).replace(thousand_separator, "")
    input_string = input_string.replace(scale_separator, ".")
    
    numbers = re.findall(r'\d+(?:\.\d+)?', input_string)
    if numbers:
        return numbers[0]
    else:
        return 0
    # return (''.join(filter(lambda i: i.isdigit(), remove_white_spaces(input_string)))).strip()


def extract_lat_long(input_string):
    """
    :param input_string: insert the path for the script which contains the lat & longitude
    :return: list of latitude & longitude
    example:
        position = extract_lat_long(response.xpath('head/script[30]//text()').get())
        item_loader.add_value('latitude', position[0])
        item_loader.add_value('longitude', position[1])
    """
    detect_the_position = input_string.rpartition("google.maps.LatLng")
    position = detect_the_position[-1]
    position = str(position)
    numbers = re.findall(r'\d+(?:\.\d+)?', position)
    if numbers:
        return numbers[:]
    else:
        return 0
    
def extract_last_number_only(input_string, thousand_separator='.', scale_separator=','):
    """[This function is used to extract the lat number from string, Common usecase is extract rent and square feet]
    Args:
        input_string ([type]): [should be string that have string + Numbers]
    Returns:
        [type]: [numbers from string ]
    """
    input_string = str(input_string).replace(thousand_separator, "")
    input_string = input_string.replace(scale_separator, ".")

    numbers = re.findall(r'\d+(?:\.\d+)?', input_string)
    if numbers:
        return numbers[-1]
    else:
        return 0

def currency_parser(input_string, external_source):
    """[This function exttract currency type from Rent or any other field that has unicode symbol]
    Args:
        input_string ([string]): [String with currency symbol]
        external_source ([string]): [String with external source name]
    Returns:
        [string]: [Currency]
    """
    currency = None
    if u'\u20ac' in input_string:
        currency = 'EUR'
    elif u'\xa3' in input_string:
        currency = 'GBP'
    elif '$' in input_string and 'australia' in external_source:
        currency = 'AUD'
    elif '$' in input_string and not 'australia' in external_source:
        currency = 'USD'
    elif 'TL' in input_string:
        currency = 'TRY'
    elif 'TRY' in input_string:
        currency = 'TRY'
    elif '₺' in input_string:
        currency = 'TRY'
    elif 'CHF' in input_string:
        currency = 'CHF'
    elif 'QAR' in input_string:
        currency = 'QAR'
    elif 'EUR' in input_string:
        currency = 'EUR'
    return currency


def format_date(input_string, date_format="%d/%m/%Y"):
    """[This function convert date from String version to python date object]
    Args:
        input_string ([string]): [String representation of date]
        date_format (str, optional): [Pass date format if default is not the case]. Defaults to "%d/%m/%Y".
    Returns:
        [python date object]: [date]
    """
    try:
        return datetime.strptime(input_string, date_format).strftime("%Y-%m-%d")
    except Exception as e:
        return input_string

def energy_label_extractor(energy_consumption_value):
    label = None
    if energy_consumption_value >= 92:
        label = 'A'
    elif energy_consumption_value >= 81 and energy_consumption_value <= 91:
        label = 'B'
    elif 69 <= energy_consumption_value <= 80:
        label = 'C'
    elif energy_consumption_value >= 55 and energy_consumption_value <= 68:
        label = 'D'
    elif energy_consumption_value >= 39 and energy_consumption_value <= 54:
        label = 'E'
    elif energy_consumption_value >= 21 and energy_consumption_value <= 38:
        label = 'F'
    elif energy_consumption_value >= 1 and energy_consumption_value <= 20:
        label = 'G'
    return label




def extract_date(input_string, date_separator="."):
    """
        This function role is to scrap the date and reformat it the desired one

        Args:
            input_string: the string which contain the date and it should be in string not list or int
            date_separator: the char that separate between the date like './-'
        Returns:
            the date and it will be formatted
    """
    german_months = {'januar': '01',
                     'februar': '02',
                     'märz': '03',
                     'april': "04",
                     'mai': '05',
                     'juni': '06',
                     'juli': '07',
                     'august': '08',
                     'september': '09',
                     'oktober': '10',
                     'november': '11',
                     'dezember': '12',
                     }
    english_months = {'january': '01',
                      'february': '02',
                      'march': '03',
                      'april': "04",
                      'may': '05',
                      'jun': '06',
                      'july': '07',
                      'august': '08',
                      'september': '09',
                      'october': '10',
                      'november': '11',
                      'december': '12'}
    months_by_all_world_languages = {
        "german_months": german_months,
        "english_months": english_months,
    }
    if input_string is not None:
        date = input_string.lower().replace(" ", "").replace("/", '.')

        for country in months_by_all_world_languages:
            for months in months_by_all_world_languages[country]:
                if months in date:
                    date_format = date.replace(months, f"{months_by_all_world_languages[country][months]}.")
                    date_extract = re.findall(r'\d+\.*\d+\.\d*', date_format)
                    if len(date_extract) > 0:
                        available_date = date_extract[0].replace(date_separator, "/")
                        if available_date.count("/") == 2:
                            return format_date(available_date)
                        elif available_date.count("/") == 1:
                            return datetime.strptime(available_date, "%m/%Y").strftime("%Y-%m")
                else:
                    date_extract = re.findall(r'\d+\.*\d+\.\d*', date)
                    if len(date_extract) > 0:
                        available_date = date_extract[0].replace(date_separator, "/")
                        if available_date.count("/") == 2:
                            return format_date(available_date)
                        elif available_date.count("/") == 1:
                            return datetime.strptime(available_date, "%m/%Y").strftime("%Y-%m")



"""
This lookup is used to identify property_type by different language
"""
property_type_lookup = {
    'Appartements': 'apartment',
    'apartment': 'apartment',
    'Appartement': 'apartment',
    'Wohnung' :'apartment',
    'Huis': 'house',
    'Haus': 'house',
    'Woning': 'house',
    'Appartamento': 'apartment',
    'Porzione di casa': 'house',
    'Terratetto': 'house',
    'Colonica': 'house',
    'Stanza': 'room',
    'Posto Letto': 'room',
    'Stanza/Posto Letto': 'room',
    'Box': 'room',
    'Stanza - Camera': 'room',
    'Camera': 'room',
    'Attico': 'room',
    'Loft': 'apartment',
    'Villa': 'house',
    'Attic': 'apartment',
    'casa indipendente': 'house',
    'appartamento di lusso': 'apartment',
    'appartamento trilocale': 'apartment',
    'Erdgeschosswohnung': 'apartment',
    'Etagenwohnung': 'apartment',
    'Dachgeschosswohnung': 'apartment',
    'Einfamilienhaus': 'house',
    'Doppelhaushälfte': 'house'

}




def extract_rent_currency(_input_string, external_source, _spider_class):
    try:
        _thousand_separator = _spider_class.thousand_separator
    except AttributeError:
        _thousand_separator = '.'
    try:
        _scale_separator = _spider_class.scale_separator
    except AttributeError:
        _scale_separator = ','
    _rent = extract_number_only(
        _input_string, thousand_separator=_thousand_separator, scale_separator=_scale_separator)
    if _rent:
        _rent = convert_to_numeric(_rent)
    _currency = currency_parser(_input_string, external_source)
    return _rent, _currency

# def square_meters_extract(_input_string):
#     return extract_number_only(remove_unicode_char(_input_string))


def is_float(_input):
    try:
        float(_input)
        res = True
    except ValueError:
        res = False
    return res


def convert_to_numeric(_input):
    _output = _input
    if isinstance(_input, int):
        pass
    elif isinstance(_input, float):
        if _input.is_integer():
            _output = int(_input)
    elif isinstance(_input, str):
        if _input.isdigit():
            _output = int(_input)
        if is_float(_input):
            if float(_input) == int(float(_input)):
                _output = int(float(_input))
            else:
                _output = float(_input)
        else:
            _output = None
    else:
        _output = None
    return _output



def extract_coordinates_regex(_string):
    location = re.findall('-?\d+\.\d+', _string)
    return location


def extract_location_from_address(address):
    try:
        responseGeocode = requests.get(
        f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        return str(longitude), str(latitude)
    except:
        return None,None


def extract_location_from_coordinates(longitude, latitude): 
    responseGeocode = requests.get(
    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
    responseGeocodeData = responseGeocode.json()
    zipcode = responseGeocodeData['address']['Postal']
    city = responseGeocodeData['address']['City']
    address = responseGeocodeData['address']['Match_addr']
    return zipcode, city, address


Amenties = {
        'pets_allowed': ['pet', 'haustiere'],
        'furnished': ['furnish', 'MÖBLIERTES'.lower()],
        'parking': ['parking', 'garage', 'parcheggio','stellplatz'],
        'elevator': ['elevator', 'aufzug', 'ascenseur', 'lift', 'aufzüg', 'fahrstuhl'],

        'balcony': ['balcon', 'balkon'],
        'terrace': ['terrace', 'terrazz', 'terras', 'terrass'],
        'swimming_pool': ['pool', 'piscine','schwimmbad'],
        'washing_machine': [' washer', 'laundry', 'washing_machine', 'waschmaschine', 'laveuse','Wasch'],
        'dishwasher': ['dishwasher', 'geschirrspüler', 'lave-vaiselle', 'lave vaiselle']
    }


def get_amenities(description, Amenties_text, item_loader):
    description = description.lower() + ' ' + Amenties_text.lower()
    pets_allowed = True if any(
        x in description for x in Amenties['pets_allowed']) else None
    furnished = True if any(
        x in description for x in Amenties['furnished']) else None
    parking = True if any(
        x in description for x in Amenties['parking']) else None
    elevator = True if any(
        x in description for x in Amenties['elevator']) else None
    balcony = True if any(
        x in description for x in Amenties['balcony']) else None
    terrace = True if any(
        x in description for x in Amenties['terrace']) else None
    swimming_pool = True if any(
        x in description for x in Amenties['swimming_pool']) else None
    washing_machine = True if any(
        x in description for x in Amenties['washing_machine']) else None
    dishwasher = True if any(
        x in description for x in Amenties['dishwasher']) else None


    x=123
    item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
    item_loader.add_value("furnished", furnished)  # Boolean
    item_loader.add_value("parking", parking)  # Boolean
    item_loader.add_value("elevator", elevator)  # Boolean
    item_loader.add_value("balcony", balcony)  # Boolean
    item_loader.add_value("terrace", terrace)  # Boolean
    item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
    item_loader.add_value("washing_machine", washing_machine)  # Boolean
    item_loader.add_value("dishwasher", dishwasher)  # Boolean
    return pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher


def description_cleaner(description):
    description = re.sub('<[^>]*>', '', description)
    description = re.sub(
                r'mail.+|call.+|contact.+|kontakt.+|ansprec.+|apply.+|\d+.\d+.\d+.\d+', "", description.lower())
    
    description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description, flags=re.MULTILINE)
    description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
    description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
    description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
    description = re.sub(r'\([0-9]+\) [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
    description = re.sub(r'[0-9]+\/+\ [0-9]+\ [0-9]+\ [0-9]+', '', description, flags=re.MULTILINE)
    description = re.sub(r'www\.[a-z]*-?[a-z]+\.[a-z]{2,}', '', description, flags=re.MULTILINE)
    
    description = remove_white_spaces(description)
    return description

def currencyExchangeRates(x, y):
    """
        # Currency Converter Rate 
        
        ### Parameters:
        x: str
            First Currency Code (ISO) - From
        y: str
            Second Currency Code (ISO) - To
            
        ### Notes:
            1. not use in a long loop
            2. not use in a lot of requests
            3. try caching response for a long time as you need  
            
        ### EX:
            >>> rate = currencyExchangeRates("eur", "hrk")
            >>> print(rate)
    """
    x, y = x.upper(), y.upper()
    
    responseCurrencyRates = requests.get(f"https://theforexapi.com/api/latest?base={x}&symbols={y}")
    responseCurrencyRatesData = responseCurrencyRates.json()
    
    if y in responseCurrencyRatesData['rates']:
        return responseCurrencyRatesData['rates'][y]
    else:
        responseCurrencyRates = requests.get(f"https://open.er-api.com/v6/latest/{x}")
        responseCurrencyRatesData = responseCurrencyRates.json()
        if y in responseCurrencyRatesData['rates']:
            return responseCurrencyRatesData['rates'][y]
        else:
            return None


# The below function take currency value and  description, and return the utilities value
# example:  item_loader.add_value('utilities', extract_utilities("€", description))


def extract_utilities(currency, description):
    if currency in description:
        find = description.rindex(currency)
        utilities = description[(find + 1):(find + 7)]
        return utilities
    else:
        return None

def energy_label_extractor(energy_consumption_value):
    label = None
    if energy_consumption_value >= 92:
        label = 'A'
    elif energy_consumption_value >= 81 and energy_consumption_value <= 91:
        label = 'B'
    elif 69 <= energy_consumption_value <= 80:
        label = 'C'
    elif energy_consumption_value >= 55 and energy_consumption_value <= 68:
        label = 'D'
    elif energy_consumption_value >= 39 and energy_consumption_value <= 54:
        label = 'E'
    elif energy_consumption_value >= 21 and energy_consumption_value <= 38:
        label = 'F'
    elif energy_consumption_value >= 1 and energy_consumption_value <= 20:
        label = 'G'
    return label


def convert_string_to_numeric(_value, _spider_class):
    try:
        _thousand_separator = _spider_class.thousand_separator
    except AttributeError:
        _thousand_separator = '.'
    try:
        _scale_separator = _spider_class.scale_separator
    except AttributeError:
        _scale_separator = ','
    output_value = extract_number_only(
        _value, thousand_separator=_thousand_separator, scale_separator=_scale_separator)
    return convert_to_numeric(output_value)


class ItemClear:
    def __init__(self, response=None, item_loader=None, item_name=None, input_value=None, input_type=None, split_list={}, replace_list={}, get_num=False, lower_or_upper=None,
                 tf_item=False, tf_words={}, tf_value=True, sq_ft=False, per_week=False):
        self.response = response
        self.item_loader = item_loader
        self.item_name = item_name
        self.input_value = input_value
        self.input_type = input_type
        self.split_list = split_list
        self.replace_list = replace_list
        self.get_num = get_num
        self.lower_or_upper = lower_or_upper
        self.tf_item = tf_item
        self.tf_words = tf_words
        self.tf_value = tf_value
        self.sq_ft = sq_ft
        self.per_week = per_week
        self.raw_data = ""
        self.start_clear()

    def extract_data(self):
        if self.input_type == 'VALUE':
            self.raw_data = self.input_value
        elif self.input_type == 'M_XPATH':
            self.raw_data = " ".join(self.response.xpath(
                self.input_value).getall()).strip()
        elif self.input_type == 'F_XPATH':
            self.raw_data = self.response.xpath(self.input_value).get(
            ) if self.response.xpath(self.input_value).get() else ""
        if self.lower_or_upper == 0:
            self.raw_data = self.raw_data.lower()
        elif self.lower_or_upper == 1:
            self.raw_data = self.raw_data.upper()

    def numeric_data(self):
        if "".join(filter(str.isnumeric, self.raw_data)):
            self.raw_data = "".join(filter(str.isnumeric, self.raw_data))
        else:
            try:
                from word2number import w2n
                self.raw_data = str(w2n.word_to_num(self.raw_data))
            except:
                pass

    def start_clear(self):
        item_loader = self.item_loader
        response = self.response
        self.extract_data()
        if self.raw_data:
            self.raw_data = self.raw_data.strip()
            if len(self.split_list) > 0:
                for k, v in self.split_list.items():
                    self.raw_data = self.raw_data.split(k)[v].strip()
            if len(self.replace_list) > 0:
                for k, v in self.replace_list.items():
                    self.raw_data = self.raw_data.replace(k, v)
            if self.get_num == True:
                self.numeric_data()
            self.raw_data = self.raw_data.strip()

        if self.item_name == 'available_date' and self.raw_data:
            from datetime import datetime
            from datetime import date
            import dateparser
            date_parsed = dateparser.parse(self.raw_data, date_formats=[
                                           "%d/%m/%Y"], languages=['en', 'es', 'fr', 'nl', 'tr'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year=today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value(self.item_name, date2)
        elif self.item_name == 'images' or self.item_name == 'floor_plan_images':
            list_of_all = [response.urljoin(
                x) for x in response.xpath(self.input_value).getall()]
            if list_of_all:
                item_loader.add_value(self.item_name, list_of_all)
            if self.item_name == 'images' and list_of_all:
                item_loader.add_value(
                    "external_images_count", len(list_of_all))
        elif self.item_name == 'energy_label' and self.raw_data:
            if self.raw_data.upper() in ['A+++', 'A++', 'A+', 'A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value(self.item_name, self.raw_data.upper())
            elif self.raw_data.isnumeric():
                energy_label = int(float(self.raw_data.replace(',', '.')))
                if energy_label >= 92:
                    item_loader.add_value("energy_label", 'A')
                elif energy_label >= 81 and energy_label <= 91:
                    item_loader.add_value("energy_label", 'B')
                elif energy_label >= 69 and energy_label <= 80:
                    item_loader.add_value("energy_label", 'C')
                elif energy_label >= 55 and energy_label <= 68:
                    item_loader.add_value("energy_label", 'D')
                elif energy_label >= 39 and energy_label <= 54:
                    item_loader.add_value("energy_label", 'E')
                elif energy_label >= 21 and energy_label <= 38:
                    item_loader.add_value("energy_label", 'F')
                elif energy_label >= 1 and energy_label <= 20:
                    item_loader.add_value("energy_label", 'G')
        elif self.tf_item == True:
            if self.raw_data:
                if self.raw_data.strip().lower() in ['yes', 'si', 'oui', 'ja', 'var', 'evet']:
                    item_loader.add_value(self.item_name, True)
                elif self.raw_data.strip().lower() in ['no', 'non', 'nee', 'yok', 'hayır']:
                    item_loader.add_value(self.item_name, False)
                elif len(self.tf_words) > 0:
                    if self.raw_data.strip().lower() == self.tf_words[False].strip().lower():
                        item_loader.add_value(self.item_name, False)
                    elif self.raw_data.strip().lower() == self.tf_words[True].strip().lower():
                        item_loader.add_value(self.item_name, True)
                else:
                    item_loader.add_value(self.item_name, self.tf_value)
        elif self.sq_ft == True and self.raw_data.isnumeric():
            item_loader.add_value(self.item_name, int(
                float(self.raw_data) * 0.09290304))
        elif self.per_week == True and self.raw_data.isnumeric():
            item_loader.add_value(
                self.item_name, int(float(self.raw_data) * 4))
        elif self.raw_data:
            item_loader.add_value(self.item_name, self.raw_data)
