# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class SwbMhDeSpider(scrapy.Spider):
    name = "swb_mh_de"
    start_urls = ['https://www.swb-mh.de/mieten/mietwohnungen']
    allowed_domains = ["swb-mh.de"]
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
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
        urls = ['https://www.swb-mh.de' + i for i in
                response.xpath('//*[@id="expose-list"]/div/div[*]/a/@href').extract()]
        for url in urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # Image
        raw = ' '.join(response.xpath('/html/body/section[1]/div/div/div/div/div/div[1]/div[*]').extract()).split()
        raw_images = []
        for i in raw:
            if 'src=' in i:
                raw_images.append(i)
        images = ['https://www.swb-mh.de' + re.search(r'/fileadmin(.*).jpg', i)[0] for i in raw_images]

        additional_images = response.css('img::attr(src)').extract()
        for i in additional_images:
            if '.jpg' in i:
                images.append(i)

        # Title
        title = response.css('.prop-headline::text').get()

        # External Id
        external_id = response.css('.prop-quickinfo::text').get().strip().split()[1]

        # longitude, latitude, zipcode, city, address
        longitude, latitude = extract_location_from_address(response.css('.bg-green-pale::text').extract()[1].strip())
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        # Description
        desc_1 = ' '.join(response.xpath('/html/body/section[2]/div/div[2]/div[2]/p/text()').extract())
        desc_2 = ' '.join(response.xpath('/html/body/section[2]/div/div[2]/div[2]/ul/li/text()').extract())
        description = desc_1 + desc_2
        description = re.sub(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})',
                             '', description)
        description = re.sub(
            r'[\S]+\.(net|com|org|info|edu|gov|uk|de|ca|jp|fr|au|us|ru|ch|it|nel|se|no|es|mil)[\S]*\s?', '',
            description)

        description = re.sub(r'[\w.+-]+@[\w-]+\.[\w.-]+', '', description)

        description = re.sub(r"[_,.*+(){}';@#?!&$/-]+\ *", " ", description)
        description = re.sub(r"[\n\r]", " ", description)
        description = re.sub(r" +", " ", description)

        # Washer,  parking, pets
        pets_allowed = washing_machine = parking = None
        if 'haustier' in description.lower():
            pets_allowed = True
        if 'garage' in description.lower():
            parking = True
        if 'waschen' in description.lower():
            washing_machine = True

        # details
        details = dict(zip(response.css('td:nth-child(1)::text').extract(),
                           [i.strip() for i in response.css('.bg-green-pale::text').extract()]))

        # Property type, square meters, Room count, Floor, bathroom count
        square_meters = room_count = floor = bathroom_count = balcony = elevator = available_date = deposit = None
        heating_cost = utilities = energy_label = None
        for i in details.keys():
            if 'wohnfläche' in i.lower():
                square_meters = int(float(details[i].strip().split()[0].replace(',', '.')))
            if 'anzahl zimmer' in i.lower():
                room_count = int(float(details[i].strip().replace(',', '.')))
            if 'etage' in i.lower():
                floor = details[i].strip()
            if 'badezimmer' in i.lower():
                bathroom_count = int(float(details[i].strip().replace(',', '.')))
            if 'balkon' in i.lower():
                if details[i] == 'Nein':
                    balcony = False
                else:
                    balcony = True
            if 'personenaufzug' in i.lower():
                if details[i] == 'Ja':
                    elevator = True
                else:
                    elevator = False
            if 'bezugstermin' in i.lower():
                available_date = '-'.join(details[i].split('.')[::-1])

            if 'kaltmiete' in i.lower():
                rent = int(float(details[i].split()[0].replace(',', '.')))

            if 'betriebskosten' in i.lower():
                utilities = int(float(details[i].split()[0].replace(',', '.')))

            if 'heizkosten' in i.lower():
                if '€' in details[i]:
                    heating_cost = int(float(details[i].split()[0].replace(',', '.')))

            if '\n                        ' in i.lower():
                if '€' in details[i]:
                    deposit = int(float(details[i].split()[0].replace('.', '').replace(',', '.')))

            if 'energieeffizenzklasse' in i.lower():
                energy_label = details[i].replace('.', '')

            if 'haustier' in i.lower():
                pets_allowed = True
            if 'garage' in i.lower():
                parking = True
            if 'waschen' in i.lower():
                washing_machine = True

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type",
                              'apartment')  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available_date)  # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        # item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost)  # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", response.css('.h3::text').get())  # String
        item_loader.add_value("landlord_phone", response.css('.w-contact a:nth-child(1)::text').get())  # String
        item_loader.add_value("landlord_email", response.css('.w-contact a::text').extract()[1])  # String

        self.position += 1
        yield item_loader.load_item()
