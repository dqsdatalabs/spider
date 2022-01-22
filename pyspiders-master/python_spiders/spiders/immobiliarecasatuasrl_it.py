# -*- coding: utf-8 -*-
# Author: Abanoub Moris
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *
import re
import json


class ImmobiliarecasatuasrlSpider(scrapy.Spider):

    name = "immobiliarecasatuasrl"
    start_urls = ['https://immobiliarecasatuasrl.it/it/Affitti/',
    'https://immobiliarecasatuasrl.it/ajax.html?azi=Archivio&lin=it&n=']
    country = 'italy' # Fill in the Country's name
    locale = 'it' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing' 

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        yield Request(self.start_urls[0], callback=self.parse,dont_filter=True)


    # 2. SCRAPING level 2
    def parse(self, response):
        cookie = response.headers.getlist('Set-Cookie')[0].decode("utf-8").split(";")[0].split("=")
        body = {
            "H_Url":"https%3A%2F%2Fimmobiliarecasatuasrl.it%2Fit%2FAffitti%2F",
            "Src_Li_Tip":"A",
            "Src_Li_Cat":"",
            "Src_Li_Cit":"",
            "Src_Li_Zon":"",
            "Src_T_Pr1":"",
            "Src_T_Pr2":"",
            "Src_T_Mq1":"",
            "Src_T_Mq2":"",
            "Src_T_Cod":"",
            "Src_Li_Ord":""
        }
        yield scrapy.FormRequest(self.start_urls[1],
        formdata=body,method='POST',
        headers={
            'cookie':cookie,
        },
         callback=self.parseApartment,dont_filter=True)
    def parseApartment(self, response,):
        

        apartments = response.css('.caption')
        for apartment in apartments:
            url = 'https://immobiliarecasatuasrl.it'+apartment.css('h4 a::attr(href)').get()
            title = apartment.css("h4 a::text").get()
            if 'ufficio' in title.lower():
                continue
            property_type = 'house' if 'attico' in title.lower() else 'apartment'
            rent = "".join(apartment.css("h5 *::text").getall())
            if rent:
                rent = re.findall(r'\d+',rent.replace('.',''))[-1]
            else:
                continue

            datausage={
                'title':title,
                'rent':rent,
                'property_type':property_type,
            }
            yield Request(url,meta=datausage,callback=self.populate_item,dont_filter=True)


    # 3. SCRAPING level 3
    def populate_item(self, response):

        title = response.meta['title']
        rent = response.meta['rent']
        property_type = response.meta['property_type']

        description = remove_white_spaces("".join(response.css('p.descrizione-immobile::text').getall()))
        external_id = re.search(r'Rif. (A\d+)',description).groups()[0]
        uta = response.css(".property-details li:contains(Spese)::text").get()
        utilities=0
        if uta:
            uta = re.findall(r'\d+',uta.replace('.',''))[-1]
            utilities = int(int(uta)/12)

        

        square_meters = response.css(".property-details li:contains(Mq)::text").get()
        if square_meters:
            square_meters = re.search(r'\d+',square_meters)[0]
        else:
            square_meters=''

        bathroom_count = int(response.css(".property-details li:contains(Bagni)::text").get()[-1])
        room_count = int(response.css(".property-details li:contains(Locali)::text").get()[-1])
        energy_label = response.css(".property-details li:contains(Classe)::text").get().split(' ')[1]
        if 'VA' in energy_label:
            energy_label=''
        images = response.css('.col-xs-3.col-sm-2.col-md-1 a::attr(href)').getall()
        images = ['https://immobiliarecasatuasrl.it'+x for x in images]


        city = response.css('.property-details li:first-child *::text').get()

        if int(rent)>0 and int(rent)<20000:
            item_loader = ListingLoader(response=response)

            # # MetaData
            item_loader.add_value("external_link", response.url) # String
            item_loader.add_value("external_source", self.external_source) # String

            item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position) # Int
            item_loader.add_value("title", title) # String
            item_loader.add_value("description", description) # String

            # # Property Details
            item_loader.add_value("city", city) # String
            #item_loader.add_value("zipcode", zipcode) # String
            #item_loader.add_value("address", address) # String
            #item_loader.add_value("latitude", latitude) # String
            #item_loader.add_value("longitude", longitude) # String
            #item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type) # String
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            #item_loader.add_value("available_date", available_date) # String => date_format also "Available", "Available Now" ARE allowed
            accessoris = "".join(response.css('.col-xs-12.caratteristica:last-child *::text').getall())
            furnished = "".join(response.css(".col-xs-12.caratteristica:contains('Non Arredato') *::text").getall())
            self.get_features_from_description(description+accessoris+furnished,item_loader)

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
            item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "EUR") # String

            #item_loader.add_value("water_cost", water_cost) # Int
            #item_loader.add_value("heating_cost", heating_cost) # Int

            item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", 'immobiliarecasatuasrl') # String
            item_loader.add_value("landlord_phone", '049 5465032') # String
            item_loader.add_value("landlord_email", 'info@immobiliarecasatuasrl.it') # String

            self.position += 1
            yield item_loader.load_item()

    def get_features_from_description(self,description,item_loader):
        description = description.lower()
        pets_allowed   ='NULLVALUE' in description
        furnished      ='arredato' in description and 'non arredato' not in description
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