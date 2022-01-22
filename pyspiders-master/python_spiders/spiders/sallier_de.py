# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class SallierDeSpider(scrapy.Spider):
    name = "sallier_de"
    start_urls = ['https://www.sallier-immobilien.de/objekte/?post_type=immomakler_object&vermarktungsart=miete&typ=wohnung']
    allowed_domains = ['sallier-immobilien.de']
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
        for url in response.css('span+ a::attr(href)').extract():
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.xpath('//*[@id="main-content"]/section[1]/div[2]/div/div/div/div/div[1]/div[1]/h1/text()').get()
        longitude, latitude =extract_location_from_address(response.xpath('//*[@id="main-content"]/section[1]/div[2]/div/div/div/div/div[1]/div[1]/p/text()').get())
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        images = [re.search(r'https://(.*)jpg' ,i)[0] for i in response.xpath('//*[@id="main-content"]/section[1]/div[1]/aside/div/div/div/div[1]').extract()[0].split('src')if 'https://' in i]

        # landlord_info
        landlord_name = re.sub(r" +", " ",response.xpath('//*[@id="main-content"]/section[1]/div[2]/div/div/div/div/div[2]/div/div/h3/text()').get()).strip()
        landlord_number = response.xpath('//*[@id="main-content"]/section[1]/div[2]/div/div/div/div/div[2]/div/div/ul[1]/li[1]/a/@href').get().split(':')[1]
        landlord_email = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', response.xpath('//*[@id="main-content"]/section[1]/div[2]/div/div/div/div/div[2]/div/div/ul[2]/li[1]/a/@href').get())[0]


        # info
        labels = response.xpath('//*[@id="main-content"]/section[1]/div[2]/div/div/div/div/div[1]/div[2]/div[2]/div[*]/div[1]/text()').extract()[::-1]
        vals = response.xpath('//*[@id="main-content"]/section[1]/div[2]/div/div/div/div/div[1]/div[2]/div[2]/div[*]/div[2]/text()').extract()[::-1]

        info = dict(zip(labels, vals))

        # external_id
        external_id = response.xpath('//*[@id="main-content"]/section[1]/div[2]/div/div/div/div/div[1]/div[2]/div[2]/div[*]/div[2]/text()').get()

        # Describtion
        description = ' '.join(response.xpath('//*[@id="tab-1-1"]/div/div/div/p[*]/text()').extract())


        # room_count, square_meters, available_date, bathroom_count, heating_cost, rent, parking
        dishwasher = washing_machine = furnished = pets_allowed = terrace = heating_cost = rent = room_count = deposit = square_meters = available_date = bathroom_count = parking = balcony = None
        living_space = 0
        usable_space = 0
        for i in info.keys():
            if 'zimmer' in i.lower():
                room_count = int(float(info[i].replace(',','.')))
            if 'wohnfläche' in i.lower():
                living_space = int(float(info[i].split()[0].replace(',','.')))
            if 'nutzfläche'  in i.lower():
                usable_space = int(float(info[i].split()[0]))
            square_meters = max(living_space, usable_space)
            if 'verfügbar ab' in i.lower():
                if '.' in info[i]:
                    available_date = '-'.join(info[i].split('.')[::-1])
            if 'badezimmer' in i.lower():
                bathroom_count = int(float(info[i].replace(',','.')))
            if 'stellplatz' in i.lower() or 'garage' in i.lower():
                parking = True
            if 'kaution' in i.lower():
                deposit = int(float(info[i].split()[0].replace('.','').replace(',','.')))
            if 'kaltmiete' in i.lower():
                rent = int(float(info[i].split()[0].replace('.','').replace(',','.')))
            if 'betriebskosten' in i.lower():
                utilities = int(float(info[i].split()[0].replace('.', '').replace(',','.')))
            if 'balkone' in i.lower():
                balcony = True
            if 'terrasse' in i.lower():
                terrace = True
        if rent is None:
            return
        # energy label
        for i in [16,18,20]:
            if response.xpath(f'//*[@id="main-content"]/section[1]/div[2]/div/div/div/div/div[1]/div[2]/div[2]/div[{str(i)}]/div[2]/div[*]/div[1]/text()').extract():
                energy_info = dict(zip(response.xpath(f'//*[@id="main-content"]/section[1]/div[2]/div/div/div/div/div[1]/div[2]/div[2]/div[{str(i)}]/div[2]/div[*]/div[1]/text()').extract(), response.xpath(f'//*[@id="main-content"]/section[1]/div[2]/div/div/div/div/div[1]/div[2]/div[2]/div[{str(i)}]/div[2]/div[*]/div[2]/text()').extract()))
                break
        energy_label = None
        for i in energy_info.keys():
            if 'klasse' in i.lower():
                energy_label = energy_info[i][0]
        if 'kennwert' in i.lower() or 'verbrauchs' in i.lower():
            energy_label = energy_label_extractor(int(float(energy_info[i].split()[0])))

        # extra_utilities
        if 'terrasse' in description.lower():
            terrace = True
        if 'balkone' in description.lower():
            balcony = True
        if 'waschkeller' in description.lower():
            washing_machine = True
        if 'stellplatz' in description.lower():
            parking = True
        if 'garage' in description.lower():
            parking = True
        if 'spülmaschine' in description.lower():
            dishwasher = True
        if 'ausgestattete' in description.lower():
            furnished = True
        if 'möbliert' in description.lower():
            furnished = True
        if 'haustier' in description.lower():
            pets_allowed = True

        description = description_cleaner(description)

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
        item_loader.add_value("latitude", str(latitude)) # String
        item_loader.add_value("longitude", str(longitude)) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", "apartment") # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        item_loader.add_value("dishwasher", dishwasher) # Boolean

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
        #item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
