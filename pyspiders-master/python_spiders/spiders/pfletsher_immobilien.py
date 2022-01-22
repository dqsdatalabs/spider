# -*- coding: utf-8 -*-
# Author: Omar Hammad
import scrapy, re, requests, json
from ..loaders import ListingLoader
from datetime import date, datetime
from ..helper import *


class PfletsherImmobilienSpider(scrapy.Spider):
    name = "pfletsher_immobilien"
    start_urls = ['https://www.ilogu.de/iframe?cw=ISC00004&cr=orange&ln=de&it=active&c=wohnung-mieten']
    allowed_domains = ["www.ilogu.de"]
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
        links = response.xpath('//*[@id="immo-table"]/tbody/tr/td/div[2]/div[2]/h4/a/@href').extract()
        for link in links:
            yield scrapy.Request('https://www.ilogu.de'+link, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # external id
        external_id = response.xpath('//*[@id="ExposeTab"]/div/div[1]/div[2]/div[1]/b/text()').extract()[0]

        # title & description
        title = response.xpath('//*[@id="cms-immo-window-size"]/div[1]/div/div/div/h3/text()').extract()[0]
        description = response.xpath('//*[@id="ExposeTab"]/div/div[1]/div[4]/p[1]/text()').extract()[0]
        
        # contact info
        contact = response.xpath('//*[@id="ExposeTab"]/div/div[2]/div')
        landlord_name = contact.xpath('./p[1]/text()').extract()[0].strip()
        #landlord_email = #response.xpath('//*[@id="ExposeTab"]/div/div[2]/div/p[4]/a/text()').extract() #contact.xpath('./p/text()').extract()
        phone_link = 'https://www.ilogu.de' + re.search(r"url:(.*?),", contact.css('script::text').extract()[0]).group(1).replace("'", "")
        landlord_number = re.search(r"phone:(.*?),", requests.get(phone_link).text).group(1).replace("'", "").replace('/', '-')

        # Address
        address = response.xpath('//*[@id="ExposeTab"]/div/div[1]/div[3]/div/text()').extract()[0].strip()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        longitude = str(longitude)
        latitude = str(latitude)

        # Images
        images = response.css('div.image-navigation').css('img::attr(src)').get()

        # Parameters
        params = response.css('ul.immo-parameter')

        rent = None
        utilities = None
        deposit = None
        square_meters = None
        balcony = None
        floor = None
        pets_allowed = None
        room_count = None
        bathroom_count = None
        energy_label = None
        washing_machine = None
        parking = None
        available_date = None
        heating_cost = None

        for param in params.css('li'):
            name = param.xpath('./div[1]/text()').extract()[0].strip()

            if name == 'Warmmiete:':
                heating_cost = int(param.xpath('./div[2]/text()').extract()[0].split(',')[0]) 

            elif name == 'Kaltmiete:':
                rent = int(param.xpath('./div[2]/text()').extract()[0].split(',')[0])

            elif name == 'Nebenkosten:':
                utilities = int(param.xpath('./div[2]/text()').extract()[0].split(',')[0])

            elif name == 'Kaution:':
                deposit = int(param.xpath('./div[2]/text()').extract()[0].split(',')[0])

            elif name == 'Wohnfläche (m²):' or name == 'Wohnfläche ca. (m²):':
                area = param.xpath('./div[2]/text()').extract()[0]
                if ',' in area:
                    square_meters = int(area.split(',')[0])
                else:
                    square_meters = int(area.split()[0])

            elif name == 'Anzahl der Zimmer:':
                room_count = int(param.xpath('./div[2]/text()').extract()[0])

            elif name == 'Anzahl der Badezimmer:':
                bathroom_count = int(param.xpath('./div[2]/text()').extract()[0])

            elif name == 'Anzahl der Balkone/Terrassen:':
                balcony = True

            elif name == 'Energieausweis (Effizienzklasse):':
                energy_label = param.xpath('./div[2]/text()').extract()[0][0]

            elif name == 'Etage:':
                floor = param.xpath('./div[2]/text()').extract()[0]

            elif name == 'Haustiere erlaubt:':
                pets_allowed = False if param.xpath('./div[2]/text()').extract()[0] == 'Nein' else True

            elif name == 'Wasch-/ Trockenraum:':
                washing_machine = True

            elif name == 'Stellplatz:':
                garage = param.xpath('./div[2]/text()').extract()[0][0]
                if garage == 'Garage':
                    parking = True

            elif name == 'Verfügbar ab Datum:':
                date = param.xpath('./div[2]/text()').extract()[0]
                now = datetime.now().strftime('%d.%m.%Y')

                date = datetime.strptime(date, '%d.%m.%Y')
                now = datetime.strptime(now, '%d.%m.%Y')
                if now < date:
                    available_date = date.strftime('%d.%m.%Y')

            if heating_cost:
                heating_cost = rent - heating_cost

        ######################################################################
        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
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
        item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", 'mietenkaufen@pfletscher-immobilien.de' ) # String

        self.position += 1
        yield item_loader.load_item()
