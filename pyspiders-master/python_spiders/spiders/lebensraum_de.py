# -*- coding: utf-8 -*-
# Author: Adham Mansour
from math import ceil

import scrapy
from ..helper import remove_unicode_char, extract_number_only, remove_white_spaces, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader

class LebensraumDeSpider(scrapy.Spider):
    name = 'lebensraum_de'
    allowed_domains = ['ivd24immobilien.de']
    start_urls = ['https://www.ivd24immobilien.de/objektlisten/index.php']  # https not http
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    keywords = {
        'pets_allowed': ['Haustiere erlaubt'],
        'furnished': ['m bliert', 'ausstattung', 'furnish'],
        'parking': ['garage', 'Stellplatz' 'Parkh user'],
        'elevator': ['fahrstuhl', 'aufzug'],
        'balcony': ['balkon'],
        'terrace': ['terrasse'],
        'swimming_pool': ['baden', 'schwimmen', 'schwimmbad', 'pool', 'Freibad'],
        'washing_machine': ['waschen', 'w scherei', 'waschmaschine','waschk che'],
        'dishwasher': ['geschirrspulmaschine', 'geschirrsp ler',]
    }
    header = {
        'Host': 'www.ivd24immobilien.de',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Content-type': 'application/x-www-form-urlencoded',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Accept-Encoding': 'gzip, deflate, br',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'User-Agent': 'Mozilla / 5.0(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
        'sec-ch-ua-platform': "Windows",
        'Origin': 'https://lebensraum.de',
        'Referer': 'https://lebensraum.de'

    }
    position = 1
    # 1. SCRAPING level 1
    def start_requests(self):

        for url in self.start_urls:
            yield scrapy.FormRequest(url, callback=self.parse, headers=self.header, method='POST', formdata={'oid': '732'})

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        next_page = response.css('.ivd24-pagination .ivd24-link::attr(data-href)').extract_first()
        current_page = response.css('.ivd24-active .ivd24-link::text').extract_first()
        rentals = response.css('.ivd24-result')

        for rental in rentals:
            external_link_id = rental.css('a::attr(href)').extract_first()
            yield scrapy.FormRequest(url='https://www.ivd24immobilien.de/objektlisten/show.php',
                                     callback=self.populate_item,
                                     formdata={'oid': '732','id': (external_link_id.split('-'))[1], 'sid': 'elimueuh72hai091chkignlpb3'},
                                     headers=self.header,
                                     method="GET",
                                     meta={'external_id' : external_link_id,
                                           'external_link' : 'https://lebensraum.de/index.php?id=27' +external_link_id,
                                           })
        if next_page is not None:
            yield scrapy.FormRequest(self.start_urls[0], callback=self.parse, headers=self.header, method='POST',
                                     formdata={'oid': '732', 'sid' :'elimueuh72hai091chkignlpb3','page' : str(int(current_page) + 1)})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        boxes = response.css('.ivd24-content-box')
        boxes_text = ''
        description = None
        for box in boxes:
            header = box.css('.ivd24-bold::text').extract_first()
            if header:
                if header == 'Objektbeschreibung':
                    description = remove_unicode_char((((' '.join(box.css(' ::text').extract()).replace('Objektbeschreibung','')).replace('\t','')).replace('\r','')))
                    boxes_text += description
                ind_box_text  = remove_unicode_char((((' '.join(box.css('::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
                boxes_text += ind_box_text
        description = remove_unicode_char(((((description).replace('\n','')).replace('\t','')).replace('\r','')))
        info_dict= {}

        keys = response.css('dt')
        value = response.css('dd')
        for n,key in enumerate(keys):
            info_dict[(key.css('::text').extract_first())]=value[n]
        title = response.css('#ivd24-expose-title::text').extract_first()
        address = info_dict['Objektanschrift']
        address = remove_white_spaces(address.css('::text').extract_first())
        latitude = None
        longitude = None
        city = None
        zipcode = None
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        property_type = 'apartment'  # response.css('::text').extract_first()

        external_id = None
        if 'Makler ObjektID' in info_dict.keys():
            external_id = info_dict['Makler ObjektID']
            external_id = (remove_white_spaces(external_id.css('::text').extract_first()))

        square_meters = None
        if 'Wohnfläche' in info_dict.keys():
            square_meters = info_dict['Wohnfläche']
            square_meters = int(extract_number_only(remove_white_spaces(square_meters.css('::text').extract_first())))


        room_count = None
        if 'Anzahl Zimmer' in info_dict.keys():
            room_count = info_dict['Anzahl Zimmer']
            room_count = int(ceil(float((remove_white_spaces(room_count.css('::text').extract_first())).replace(',','.'))))


        available_date = None
        if 'Verfügbar ab' in info_dict.keys():
            available_date = info_dict['Verfügbar ab']
            available_date = remove_white_spaces(available_date.css('::text').extract_first())
            available_date = available_date.split('.')
            available_date = available_date[-1] +'-'+available_date[1]+'-'+available_date[0]



        images = response.css("img.ivd24-thumbnail::attr(src)").getall()

        floor_plan_images = [i for i in images if 'grundriss' in i]
        rent = None
        if 'Kaltmiete zzgl. NK' in info_dict.keys():
            rent = info_dict['Kaltmiete zzgl. NK']
            rent = int(extract_number_only(remove_white_spaces(rent.css('::text').extract_first())))


        heating_cost = None
        if 'Nebenkosten' in info_dict.keys():
            heating_cost = info_dict['Nebenkosten']
            heating_cost = int(extract_number_only(remove_white_spaces(heating_cost.css('::text').extract_first())))

        deposit = None
        if 'Kaution' in info_dict.keys():
            deposit = info_dict['Kaution']
            deposit = int(extract_number_only(remove_white_spaces(deposit.css('::text').extract_first())))


        floor = None
        if 'Etage' in info_dict.keys():
            floor = info_dict['Etage']
            floor = extract_number_only(remove_white_spaces(floor.css('::text').extract_first()))

        pets_allowed = None
        if 'Haustiere erlaubt' in info_dict.keys():
            pets_allowed = info_dict['Haustiere erlaubt']
            pets_allowed = (remove_white_spaces(pets_allowed.css('::attr(src)').extract_first()))
        if pets_allowed == 'https://www.ivd24immobilien.de/images/ivd24immobilien_hacken_gruen.png':
            pets_allowed = True
        elif pets_allowed == 'https://www.ivd24immobilien.de/images/ivd24immobilien_x.png':
            pets_allowed = False

        furnished = None
        if any(word in description.lower() for word in self.keywords['furnished']):
            furnished = True

        parking = None
        if any(word in description.lower() for word in self.keywords['parking']):
            parking = True

        elevator = None
        if any(word in description.lower() for word in self.keywords['elevator']):
            elevator = True

        balcony = None
        if any(word in description.lower() for word in self.keywords['balcony']):
            balcony = True

        terrace = None
        if any(word in description.lower() for word in self.keywords['terrace']):
            terrace = True

        swimming_pool = None
        if any(word in description.lower() for word in self.keywords['swimming_pool']):
            swimming_pool = True

        washing_machine = None
        if any(word in description.lower() for word in self.keywords['washing_machine']):
            washing_machine = True

        dishwasher = None
        if any(word in description.lower() for word in self.keywords['dishwasher']):
            dishwasher = True

        landlord_name = None
        landlord_email = None
        landlord_phone = None

        # # MetaData
        item_loader.add_value("external_link", response.meta['external_link'])  # String
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
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

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
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'lebensraum') # String
        item_loader.add_value("landlord_phone", '+ 49(0)761 - 7333337') # String
        item_loader.add_value("landlord_email", 'freiburg@homecompany.de') # String

        self.position += 1
        yield item_loader.load_item()
