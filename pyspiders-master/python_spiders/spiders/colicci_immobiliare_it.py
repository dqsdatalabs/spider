# -*- coding: utf-8 -*-
# Author: Adham Mansour
import re

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only, extract_location_from_address, \
    extract_location_from_coordinates
from ..loaders import ListingLoader


class colicci_immobiliareItSpider(scrapy.Spider):
    name = 'colicci_immobiliare_it'
    allowed_domains = ['colicci-immobiliare.it']
    start_urls = ['http://www.colicci-immobiliare.it/it/Affitti/']  # https not http
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1
    def parse(self, response ):
        cookie = (str((response.headers.getlist('Set-Cookie'))[0])).split(';')[0]
        cookie = cookie[2:]
        yield Request(url='http://www.colicci-immobiliare.it/ajax.html?azi=Archivio&lin=it&n=',
                      callback=self.parse1,
                      meta={'cookie' : cookie})
    # 1. SCRAPING level 1
    def parse1(self,response):
        header = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language':'en-US,en;q=0.9,ar-EG;q=0.8,ar;q=0.7',
                    'Connection': 'keep-alive',
                    'Content-Length': '179',
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'Cookie': f'{response.meta["cookie"]}; verifica_cookie',
                    'DNT': '1',
                    'Host':'www.colicci-immobiliare.it',
                    'Origin': 'http://www.colicci-immobiliare.it',
                    'Referer': 'http://www.colicci-immobiliare.it/it/Affitti/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36',
                    'X-Requested-With': 'XMLHttpRequest'
                    }

        for url in self.start_urls:
            yield scrapy.FormRequest('http://www.colicci-immobiliare.it/ajax.html?azi=Archivio&lin=it&n=',
                                       callback=self.parse2,
                                       method= "POST",
                                       headers=header,
                                       formdata={'Src_Li_Tip': 'A',
                                                    'Src_Li_Cat': '',
                                                    'Src_Li_Cit': '',
                                                    'Src_Li_Zon': '',
                                                    'Src_T_Cod': '',
                                                    'Src_T_Pr1': '',
                                                    'Src_T_Pr2': '',
                                                    'Src_T_Mq1': '',
                                                    'Src_T_Mq2': '',
                                                    'Src_Li_Ord': '',
                                                    'H_Url': 'http://www.colicci-immobiliare.it/it/Affitti/'})

    # 2. SCRAPING level 2
    def parse2(self, response, **kwargs):
        html_regex = re.findall('":"(.+)"}',response.text)
        html_regex = re.findall('Affitti/([\w|/|\?|=\d|-]+)',html_regex[0])
        for rental in html_regex:
            yield Request(url='http://www.colicci-immobiliare.it/it/Affitti/'+rental,
                          callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('h1::text').extract_first()
        description = remove_unicode_char((((' '.join(response.css('.descrizione ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
        city = (re.findall('Affitti/([\w]+)',response.url))[0]
        longitude, latitude = extract_location_from_address(city)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        try:
            longitude, latitude = extract_location_from_address(title)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        except:
            pass
        if latitude and longitude:
            latitude = str(latitude)
            longitude = str(longitude)
        property_type = 'apartment'

        infos = response.css('.carat')
        info_dict = {}
        for info in infos:
            header = info.css('.caratLabel::text').extract_first()
            if header:
                value = info.css('.caratValue::text').extract_first()
                if value is None:
                    value = ''
            info_dict[header]= value
        square_meters = None  # METERS #int(response.css('::text').extract_first())
        if 'Mq' in info_dict.keys():
            square_meters = info_dict['Mq']
            if square_meters:
                square_meters = int(extract_number_only(extract_number_only(square_meters)))

        utilities = None
        if 'Spese condominiali' in info_dict.keys():
            utilities = info_dict['Spese condominiali']
            if utilities:
                utilities = int(extract_number_only(extract_number_only(utilities)))


        room_count = None  # int(response.css('::text').extract_first())
        if 'Locali' in info_dict.keys():
            room_count = info_dict['Locali']

        bathroom_count = None  # int(response.css('::text').extract_first())
        if 'Bagni' in info_dict.keys():
            bathroom_count = info_dict['Bagni']


        images = response.css('.fotorama img::attr(src)').extract()
        images = ['http://www.colicci-immobiliare.it/'+i for i in images]

        rent = None  # int(response.css('::text').extract_first())
        if 'Prezzo' in info_dict.keys():
            rent = info_dict['Prezzo']
            if rent:
                rent = int(extract_number_only(extract_number_only(rent)))

        energy_label = None
        if 'Classe' in info_dict.keys():
            energy_label = info_dict['Classe']

        floor = None
        if 'Piano' in info_dict.keys():
            floor = info_dict['Piano']

        furnished = None
        if 'Arredi' in info_dict.keys():
            furnished = info_dict['Arredi']
            if furnished == 'Arredato':
                furnished = True
            elif furnished == 'Non Arredato':
                furnished = False

        parking = None
        if 'Posti auto' in info_dict.keys():
            parking = True

        elevator = None
        if 'Accessori' in info_dict.keys():
            if 'Ascensore' in info_dict['Accessori']:
                elevator = True
        #
        # balcony = None
        if 'balcone' in description.lower():
            balcony = True
        #
        terrace = None
        if 'terrazza' in description.lower():
            terrace = True
        #
        swimming_pool = None
        if 'piscina' in description.lower():
            swimming_pool = True


        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int
        #
        # item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed
        #
        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'colicci immobiliare') # String
        item_loader.add_value("landlord_phone", '+39 02 48916083') # String
        item_loader.add_value("landlord_email", 'info@colicci.it') # String

        self.position += 1
        yield item_loader.load_item()
