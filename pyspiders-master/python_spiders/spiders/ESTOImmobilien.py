# -*- coding: utf-8 -*-
# Author: Muhammad Ahmad Hesham
from datetime import datetime
from math import ceil

from dateutil.parser import parse
from datetime import datetime
import scrapy

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, remove_white_spaces
from ..loaders import ListingLoader


class EstoimmobilienSpider(scrapy.Spider):
    name = "ESTOImmobilien"
    start_urls = ['https://system.ivd24immobilien.de/objektliste/1289?page=1&sort=id&standort=&objektart=&vermarktungsart=&preisbis=&zimmerab=&flaecheab=&baujahrab=']
    allowed_domains = []
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    keywords = {
        'pets_allowed': ['Haustiere erlaubt'],
        'furnished': ['m bliert', 'ausstattung'],
        'parking': ['garage', 'Stellplatz' 'parkh user', 'Parkplatz'],
        'elevator': ['fahrstuhl', 'aufzug'],
        'balcony': ['balkon'],
        'terrace': ['terrasse'],
        'swimming_pool': [' baden', 'schwimmen', 'schwimmbad', 'pool', 'Freibad'],
        'washing_machine': ['waschen', 'w scherei', 'waschmaschine', 'waschk che'],
        'dishwasher': ['geschirrspulmaschine', 'geschirrsp ler', ],
        'floor': ['etage'],
        'bedroom': ['Schlafzimmer']
    }
    position = 1
    headers = {
        'Host': 'system.ivd24immobilien.de',
        'Origin': 'https://www.stockgruppe.de',
        'Referer': 'https://www.stockgruppe.de/',
    }
    duplicate = {}

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        form_body = 'page=1&sort=id&standort=&objektart=&vermarktungsart=&preisbis=&zimmerab=&flaecheab=&baujahrab='
        for url in self.start_urls:
            yield scrapy.FormRequest(url, callback=self.parse,body=form_body, method='GET',headers=self.headers)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        rentals = response.css('.expose-link::attr(data-id)').extract()
        for rental in rentals:
            yield scrapy.FormRequest(url=f'https://system.ivd24immobilien.de/objektliste/expose/{rental}?objektliste=1289',
                          callback=self.populate_item,method="GET",headers=self.headers)
    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_id = response.css('.col-md-8 strong::text').extract_first()
        external_id = (external_id.split(','))[0]
        external_id = (external_id.split(':'))[1].strip()
        title = response.css('h1.expose-title::text').extract_first()
        if not 'geschäftshaus' in title.lower():
            description = ((((' '.join(response.css('#expose-descriptions p ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
            address = response.css('div.col-10.col-sm-10.pl-5.pl-md-4 span::text').extract_first()
            city = (address.split(' ('))[0]
            zipcode = extract_number_only(address)
            longitude, latitude = extract_location_from_address(address)
            list_items = response.css('.col-md-12.row')
            list_items_dict = {}
            for list_item in list_items:
                head_val = list_item.css('div::text').extract()
                head_val = [remove_unicode_char(remove_white_spaces(i)) for i in head_val]
                head_val = [i for i in head_val if i !='']

                if len(head_val) == 2:
                    header = head_val[0]
                    value = head_val[1]
                    list_items_dict[header] = value
            property_type = None  # response.css('::text').extract_first()
            if 'Objektart' in list_items_dict.keys():
                property_type = list_items_dict['Objektart']
            if 'wohnung' in property_type.lower() :
                property_type = 'apartment'
            elif 'haus' in property_type.lower() :
                property_type = 'house'
            else:
                property_type = None
            if property_type:
                square_meters = None  # METERS #int(response.css('::text').extract_first())
                if 'Wohnfl che' in list_items_dict.keys():
                    square_meters = list_items_dict['Wohnfl che']
                    square_meters = int(ceil(float(extract_number_only(square_meters))))
                room_count = None  # int(response.css('::text').extract_first())
                if 'Anzahl Zimmer' in list_items_dict.keys():
                    room_count = list_items_dict['Anzahl Zimmer']
                    room_count = int(ceil(float(extract_number_only(room_count))))
                bathroom_count = None  # int(response.css('::text').extract_first())
                if 'Anzahl Badezimmer' in list_items_dict.keys():
                    bathroom_count = list_items_dict['Anzahl Badezimmer']
                    bathroom_count = int(ceil(float(extract_number_only(bathroom_count))))

                available_date = None  # response.css('.availability .meta-data::text').extract_first()
                if 'Verf gbar ab' in list_items_dict.keys():
                    possible_date = list_items_dict['Verf gbar ab'].split(' ')
                    for date in possible_date:
                        if 'sofort' in date.lower():
                            available_date = datetime.now().strftime("%Y-%m-%d")
                            break
                        else:
                            try:
                                available_date = parse(date).strftime("%Y-%m-%d")
                                break
                            except:
                                pass
                images = response.css('.fl-gallery::attr(href)').extract()
                floor_plan_images = response.css('.fl-gallery')
                floor_plan_images = [i.css('::attr(href)').extract_first() for i in floor_plan_images if 'grundriss' in (i.css('::attr(data-title)').extract_first()).lower()]
                rent = None  # response.css('::text').extract_first()
                if 'Kaltmiete' in list_items_dict.keys():
                    rent = list_items_dict['Kaltmiete']
                    rent = int(ceil(float(extract_number_only(rent))))
                elif 'Miete inkl. Nebenkosten' in list_items_dict.keys():
                    rent = list_items_dict['Miete inkl. Nebenkosten']
                    rent = int(ceil(float(extract_number_only(rent))))
                if rent:
                    deposit = None
                    if 'Kaution' in list_items_dict.keys():
                        deposit = list_items_dict['Kaution']
                        deposit = int(ceil(float(extract_number_only(deposit))))
                    utilities = None
                    if 'Nebenkosten' in list_items_dict.keys():
                        utilities = list_items_dict['Nebenkosten']
                        utilities = int(ceil(float(extract_number_only(utilities))))
                    energy_label = response.css('td:contains("Wertklasse") + td::text').extract_first()

                    floor = None  # response.css('::text').extract_first()
                    if 'Etage' in list_items_dict.keys():
                        utilities = list_items_dict['Etage']

                    pets_allowed = None
                    if 'Haustiere' in list_items_dict.keys():
                        pets_allowed = list_items_dict['Haustiere']
                        if 'nein' in pets_allowed.lower():
                            pets_allowed = False
                        elif 'ja' in pets_allowed.lower():
                            pets_allowed = True
                        else:
                            pets_allowed = None
                    elif 'keine haustiere' in description.lower():
                        pets_allowed = False
                    elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['pets_allowed']):
                        pets_allowed = True

                    furnished = None
                    if any(word in remove_unicode_char(description.lower()) for word in self.keywords['furnished']):
                        furnished = True

                    parking = None
                    if 'Anzahl Stellpl tze' in list_items_dict.keys():
                        parking = list_items_dict['Anzahl Stellpl tze']
                        parking = int(ceil(float(extract_number_only(parking.replace('.',',')))))
                        if parking > 0:
                            parking = True
                        else:
                            parking = False
                    elif any(word in remove_unicode_char(description.lower()) for word in self.keywords['parking']):
                        parking = True

                    elevator = None
                    if any(word in remove_unicode_char(description.lower()) for word in self.keywords['elevator']):
                        elevator = True

                    balcony = None
                    if any(word in remove_unicode_char(description.lower()) for word in self.keywords['balcony']):
                        balcony = True

                    terrace = None
                    if any(word in remove_unicode_char(description.lower()) for word in self.keywords['terrace']):
                        terrace = True

                    swimming_pool = None
                    if any(word in remove_unicode_char(description.lower()) for word in self.keywords['swimming_pool']):
                        swimming_pool = True

                    washing_machine = None
                    if any(word in remove_unicode_char(description.lower()) for word in self.keywords['washing_machine']):
                        washing_machine = True

                    dishwasher = None
                    if any(word in remove_unicode_char(description.lower()) for word in self.keywords['dishwasher']):
                        dishwasher = True

                    landlord_name = response.css('#expose-sidebar .widget::text').extract()
                    landlord_name = [i.strip() for i in landlord_name if 'immobilien' not in i.lower() and i.strip() !=''][0]
                    landlord_email = 'esto@stockgruppe.de'
                    landlord_phone = response.css('#expose-sidebar .widget .toggle-phone::attr(data-phone)').extract_first()
                    description = ((((' '.join(response.css('#expose-descriptions > div:nth-child(1) > div > p::text').extract()).replace('\n','')).replace('\t', '')).replace('\r', '')))

                    # # MetaData
                    item_loader.add_value("external_link", 'https://www.stockgruppe.de/69/immobilienangebote/#'+str(self.position)) # String
                    item_loader.add_value("external_source", self.external_source)  # String

                    item_loader.add_value("external_id", external_id) # String
                    item_loader.add_value("position", self.position)  # Int
                    item_loader.add_value("title", title) # String
                    item_loader.add_value("description", description) # String

                    # # Property Details
                    item_loader.add_value("city", city) # String
                    item_loader.add_value("zipcode", zipcode) # String
                    item_loader.add_value("address", address) # String
                    item_loader.add_value("latitude", str(latitude)) # String
                    item_loader.add_value("longitude", str(longitude)) # String
                    item_loader.add_value("floor", floor) # String
                    item_loader.add_value("property_type", property_type) # String
                    item_loader.add_value("square_meters", square_meters) # Int
                    item_loader.add_value("room_count", room_count) # Int
                    item_loader.add_value("bathroom_count", bathroom_count) # Int

                    item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

                    item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                    item_loader.add_value("furnished", furnished) # Boolean
                    item_loader.add_value("parking", parking) # Boolean
                    item_loader.add_value("elevator", elevator) # Boolean
                    item_loader.add_value("balcony", balcony) # Boolean
                    item_loader.add_value("terrace", terrace) # Boolean
                    item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                    item_loader.add_value("washing_machine", washing_machine) # Boolean
                    item_loader.add_value("dishwasher", dishwasher) # Boolean

                    # # Images
                    item_loader.add_value("images", images) # Array
                    item_loader.add_value("external_images_count", len(images)) # Int
                    item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                    # # Monetary Status
                    item_loader.add_value("rent", rent) # Int
                    item_loader.add_value("deposit", deposit) # Int
                    # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                    item_loader.add_value("utilities", utilities) # Int
                    item_loader.add_value("currency", "EUR") # String

                    # item_loader.add_value("water_cost", water_cost) # Int
                    # item_loader.add_value("heating_cost", heating_cost) # Int

                    item_loader.add_value("energy_label", energy_label) # String

                    # # LandLord Details
                    item_loader.add_value("landlord_name", landlord_name) # String
                    item_loader.add_value("landlord_phone", landlord_phone) # String
                    item_loader.add_value("landlord_email", landlord_email) # String

                    self.position += 1
                    yield item_loader.load_item()
