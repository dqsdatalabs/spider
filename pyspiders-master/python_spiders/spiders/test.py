# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from gevent.libev.corecext import callback

from ..loaders import ListingLoader
from ..helper import *
import re
import json

from scrapy.loader import ItemLoader
from ..items import ListingItem

class AhmedSpider(scrapy.Spider):
    name = "ivd24immobilien"
    start_urls = ['https://anbieter.ivd24immobilien.de/hansen-blum-immobilien-gmbh']
    # allowed_domains = ["hansen-blum-immobilien.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        apartments_urls=response.css(".bg-img a::attr(href)").getall()

        for apartment_url in apartments_urls:
            yield scrapy.Request(url="https://anbieter.ivd24immobilien.de/"+apartment_url,callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):


        item_loader = ListingLoader(response=response)

        # Your scraping code goes here
        # Dont push any prints or comments in the code section
        # if you want to use an item from the item loader uncomment it
        # else leave it commented
        # Finally make sure NOT to use this format
        #    if x:
        #       item_loader.add_value("furnished", furnished)
        # Use this:
        #   balcony = None
        #   if "balcony" in description:
        #       balcony = True

        # # Enforces rent between 0 and 40,000 please dont delete these lines
        description=""
        room_count = None
        bathroom_count = None
        floor = None
        parking = True
        elevator = True
        balcony = True
        washing_machine = True
        dishwasher = True
        utilities = None
        terrace = True
        furnished = True
        property_type = None
        energy_label = None
        deposit = None
        available = None
        pets_allowed = True
        square_meters = None
        swimming_pool = 1
        external_id = None
        rent = None
        title = str(response.css(".col-md-12 h1::text").get())
        if(response.css(".col-md-12 p::text").get()>='0'and response.css(".col-md-12 p::text").get()<='z'):
            description=response.css(".col-md-12 p::text").get()
        else:
            description="None"
        address=response.css(".col-11 span::text").get()
        city=response.css(".expose-breadcrumb a::text").getall()[1]
        if str(response.css(".col-5 h3::text").get().strip(' ')[0])>='0'and str(response.css(".col-5 h3::text").get().strip(' ')[0])<='z':
            square_meters=response.css(".col-5 h3::text").get()
            square_meters=re.findall(r'\d+(?:\.\d+)?', square_meters)
            square_meters=square_meters[0]
            if square_meters[0] >= 'A' and square_meters[0]<='z':
                square_meters=1
            else:
                square_meters = int(square_meters)
        else:
            square_meters=1
        if square_meters==0:
            square_meters=1
        rent=response.css('.short-info h3::text').get()
        rent=int(extract_number_only(rent, '.', ','))
        lat=str(response.xpath("//script[contains(., 'ivd24')]/text()").extract()).split('"')[7]
        lon=str(response.xpath("//script[contains(., 'ivd24')]/text()").extract()).split('"')[9]
        landlord_number = '0681-876280'
        landlord_name = 'Ms. Filothea Kallenborn'
        landlord_email='vertrieb@hansen-blum-immobilien.de'
        currency="EUR"
        deposit=int(extract_number_only(response.xpath('//div[@class="col-md-6 row"]/div/text()').extract()[6].strip(' '),'.',','))
        # external_id=response.css('.expose-map-container strong::text').get().split(':')[-1].strip(' ')
        try:
            room_count=response.xpath('//div[@class="expose-fields-container"]/div/div/div/div/text()').extract()[4].strip(' ').split(',')[0]
            if(room_count[0]>='A' and room_count<='z'):
                room_count=1
            else:
                room_count=float(room_count)
        except:
            room_count=1
        try:
            bathroom_count=int(float(response.xpath('//div[@class="expose-fields-container"]/div/div/div/div/text()').extract()[6].strip(' ').split(',')[0]))
            if (room_count[0] >= 'A' and room_count <= 'z'):
                bathroom_count = 1
            else:
                bathroom_count = float(bathroom_count)
        except:
            bathroom_count=1
        # floor=1
        is_rent=response.xpath('//div[@class="row short-info mt-3 mt-md-0"]/div/span/text()').extract()[0]
        #
        images=response.xpath("//div[@class='row']/div/div/div/div/div/a/img/@src").extract()

        if rent==None:
            rent=0
        # if address==None:
        #     address=""
        # if external_id==None:
        #     external_id=" "
        # if bathroom_count==None:
        #     bathroom_count=0
        # if room_count == None:
        #     room_count = 0

        if "Miete" not in is_rent:
            return

        # # if int(rent[0]) <= 0 and int(rent[0]) > 40000:
        # #     return
        #
        if "Wohnung" in description.lower():
            property_type = "apartment"
        else :
            property_type = "office"

        # if "Hubgarage" in description.lower() :
        #     parking=True
        # floor_plan_images=response.css(".btn btn-success btn-block img::attr(src)").get()
        # # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String
        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String
        # #
        # # # # Property Details
        item_loader.add_value("city", city) # String
        zipcode="66121"
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", lat) # String
        item_loader.add_value("longitude", lon) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type",property_type ) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int
        # #
        # # # item_loader.add_value("available_date", available_date) # String => date_format
        # #
        # pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, dishwasher=get_amenities(description,description,item_loader)
        if ("pet" not in description.lower())  and ("haustiere" not in description.lower()):
            pets_allowed=-1
        if('MÖBLIERTES'.lower() not in description.lower()) and ('furnish' not in description.lower()):
            furnished=-1
        if('parking' not in description.lower()) and ('garage' not in description.lower()) and \
                ('parcheggio' not in description.lower()) and ('stellplatz' not in description.lower()):
            parking=-1

        if('elevator' not in description.lower()) and('aufzug' not in description.lower()) and  \
            ('ascenseur' not in description.lower()) and ('lift' not in description.lower()) and \
             ('aufzüg' not in description.lower()) and ('fahrstuhl' not in description.lower()):
            elevator=-1

        if('balcon' not in description.lower()) and ('balkon' not in description.lower()):
            balcony=-1

        if('terrace' not in description.lower()) and ('terrazz' not in description.lower()) \
            and ('terras' not in description.lower()) and ('terrass' not in description.lower()):
            terrace=-1

        if('pool' not in description.lower()) and ('piscine' not in description.lower()) \
            and('schwimmbad' not in description.lower()):
            swimming_pool=-1

        if('washer' not in description.lower()) and ('laundry' not in description.lower())\
            and('washing_machine' not in description.lower()) and('waschmaschine' not in description.lower())\
            and('laveuse' not in description.lower()) and('Wasch'.lower() not in description.lower()):
            washing_machine=-1
        if('dishwasher' not in description.lower()) and('geschirrspüler' not in description.lower())\
            and('lave-vaiselle' not in description.lower()) and('lave vaiselle' not in description.lower()):
            dishwasher=-1
        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean
        # # # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images",images) # Array
        # #
        # # # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        # #
        item_loader.add_value("deposit", deposit) # Int
        # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # # #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String
        # #
        # # #item_loader.add_value("water_cost", water_cost) # Int
        # # #item_loader.add_value("heating_cost", heating_cost) # Int
        # #
        # # #item_loader.add_value("energy_label", energy_label) # String
        # #
        # # # # LandLord Details
        # #
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
