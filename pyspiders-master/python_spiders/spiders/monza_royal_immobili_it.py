# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re


class MonzaRoyalImmobiliSpider(scrapy.Spider):

    name = "monza_royal_immobili"
    start_urls = ['https://monza.royal-immobili.it/immobili/?contratto=Affitto&prezzo_da=&prezzo_a=&sup_da=&sup_a=&numero_camere=-&numero_bagni=-&tipologia=Appartamento&ordine=-&submit=Cerca&k_hid_quicksearch=quicksearch&nc=1#quicksearchhttps://bergamo.royal-immobili.it/immobili/?contratto=Affitto&prezzo_da=&prezzo_a=&sup_da=&sup_a=&numero_camere=-&numero_bagni=-&tipologia=Appartamento&ordine=-&submit=Cerca&k_hid_quicksearch=quicksearch&nc=1#quicksearch',
               'https://monza.royal-immobili.it/immobili/?contratto=Affitto&prezzo_da=&prezzo_a=&sup_da=&sup_a=&numero_camere=-&numero_bagni=-&tipologia=-&ordine=-&submit=Search+for&k_hid_quicksearch=quicksearch&nc=1&pg=2'
]
    country = 'italy' # Fill in the Country's name
    locale = 'it' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield Request(url, callback=self.parse,dont_filter=True)

    # 2. SCRAPING level 2
    def parse(self, response):
        apartments = response.css('.row.no-gutters a')
        for apartment in apartments:
            url = apartment.css('::attr(href)').get()
            title = apartment.css("[class*='title']::text").get()
            rent = apartment.css("[class*='price']::text").get()
            if rent:
                rent = re.search(r'\d+',rent.replace(",00",'').replace('.',''))[0]
            else:
                continue
            datausage={
                'title':title,
                'rent':rent
            }
            yield Request(url,meta=datausage,callback=self.populate_item,dont_filter=True)


    # 3. SCRAPING level 3
    def populate_item(self, response):

        description = remove_white_spaces("".join(response.css('.mt-3.mt-md-4 p::text').getall()))
        property_type = response.css(".pxp-sp-key-details-item:contains('ipologia propriet') *.pxp-sp-kd-item-value::text").get()
        property_type = self.get_property_type(property_type,description)
        if property_type=='':
            return
        
        title = response.meta['title']
        rent = response.meta['rent']
        square_meters = "".join(response.css(".pxp-sp-kd-item-value:contains('m')::text").getall())
        if square_meters:
            square_meters = re.search(r'\d+',square_meters)[0]
        else:
            square_meters=''

        bathroom_count = response.css(".pxp-sp-key-details-item:contains('agni') *.pxp-sp-kd-item-value::text").get()
        room_count = response.css(".pxp-sp-key-details-item:contains('amere') *.pxp-sp-kd-item-value::text").get()
        energy_label =response.css(".pxp-sp-key-details-item:contains('energetic') *.pxp-sp-kd-item-value::text").get()
        if energy_label:
            energy_label=energy_label[0]
        floor = response.css(".pxp-sp-key-details-item:contains('iano') *.pxp-sp-kd-item-value::text").get()
        if floor:
            floor=floor.split(' ')[0]
        else:
            floor=''
        
        images = response.css('.carousel-item a::attr(href)').getall()
        
        latlng = extract_lat_long(response.css("script:contains(initMap)").get())
        latitude = latlng[0]
        longitude=latlng[1]
        zipcode, city, address = extract_location_from_coordinates(longitude,latitude)

        if int(rent)>0 and int(rent)<20000:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url) # String
            item_loader.add_value("external_source", self.external_source) # String

            #item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
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

            #item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed

            self.get_features_from_description(description,item_loader)
            
            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            #item_loader.add_value("parking", parking) # Boolean
            #item_loader.add_value("elevator", elevator) # Boolean
            #item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            #item_loader.add_value("washing_machine", washing_machine) # Boolean
            #item_loader.add_value("dishwasher", dishwasher) # Boolean

            # # Images
            item_loader.add_value("images", images) # Array
            item_loader.add_value("external_images_count", len(images)) # Int
            #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # # Monetary Status
            item_loader.add_value("rent", rent) # Int
            #item_loader.add_value("deposit", deposit) # Int
            #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            #item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'Royal Immobili') # String
            item_loader.add_value("landlord_phone", '0393900383') # String
            item_loader.add_value("landlord_email", 'monzacentro@royal-immobili.it') # String

            self.position += 1
            yield item_loader.load_item()

    def get_features_from_description(self,description,item_loader):
        description = description.lower()
        pets_allowed   ='NULLVALUE' in description
        furnished      ='arredato' in description
        parking        ='NULLVALUE' in description
        elevator       ='ascensore' in description
        balcony        ='balcon' in description
        terrace        ='terrazz' in description
        swimming_pool  ='NULLVALUE' in description
        washing_machine='NULLVALUE' in description
        dishwasher     ='NULLVALUE' in description

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean
        return pets_allowed,furnished,parking,elevator,balcony,terrace,swimming_pool,washing_machine,dishwasher     
    
    def get_property_type(self,property_type,description):

        if property_type and ('appartamento' in property_type.lower() or 'appartamento' in description.lower()):
            property_type = 'apartment'
        elif property_type and 'ufficio' in property_type.lower():
            property_type=""
        else:
            if not property_type:
                property_type=''
            else:
                property_type = 'house'
        return property_type