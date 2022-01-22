# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import string

import scrapy
from ..loaders import ListingLoader


class ButscherNetSpider(scrapy.Spider):
    name = "butscher_net"
    start_urls = ['https://www.butscher.net/immobilienangebote/thueringen/mietangebote']
    allowed_domains = ["butscher.net"]
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
        all_urls = response.xpath('//*[@id="article-90"]//@href').extract()
        used_url = []
        for i in range(2, len(all_urls)):
            if (i % 2) == 0 :
                used_url.append("https://www.butscher.net" + all_urls[i])

        for url in used_url:
            yield scrapy.Request(url=url,
                                 callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # rent
        rent_array = response.css('.preise-kaufpreis .value-string::text').get()
        if rent_array:
            rent = int(float(''.join(rent_array).replace(' €', '').replace(',', '.')))
        else:
            return

        # Read Images
        images_list = response.xpath('//*[@id="Slider_immo"]/div[2]/ol//@href').extract()
        images = []
        for i in images_list:
            images.append("https://www.butscher.net/" + i)

        # City
        city = 'thueringen'

        # Description
        description = response.xpath('//*[@id="tabtwo"]/p[2]/text()').get()

        # miscellaneous
        miscellaneous_array = response.xpath('''//*[@id="tabtwo"]/p[3]/text()''').extract()
        miscellaneous = " ".join(miscellaneous_array).replace('\n', ' ')


        # title
        title = response.css('h1::text').get()

        # ID
        external_id = response.css('.obkt_number::text').get()


        # currency
        currency = 'EUR'

        # Square meters
        square_meters_array = response.css('.field~ .field+ .field .value-string::text').extract()
        square_meters = int(float(square_meters_array[0].replace(' m²','')))

        # Room Count
        room_array = response.css('.field:nth-child(2) .value-string::text').extract()
        room_count = int(float(room_array[0]))

        # Multi-extract commands
        labels_array = response.css('#tabOne .label::text').extract()
        values_array = response.css('#tabOne .value-string::text').extract()

        # Address
        location = ''
        for i in range(len(labels_array)):
            if labels_array[i]=='Ort':
                location = values_array[i]+"-"

        address = response.xpath('''//*[@id="tabthree"]/p[2]/text()''').get()


        # Zip code
        zipcode = None
        for i in range(len(labels_array)):
            if labels_array[i] == 'Postleitzahl':
                zipcode = values_array[i]

        # Deposit
        deposit = None
        for i in range(len(labels_array)):
            if labels_array[i] == 'Kaution':
                deposit = int(float(values_array[i].replace(' €','')))

        # Heating cost
        heating_cost = None
        for i in range(len(labels_array)):
            if labels_array[i] == 'Nebenkosten':
                heating_cost = int(values_array[i].replace(' €',''))

        # Property_type
        types = ["house", "apartment", "student_apartment", "studio", "room"]
        for ch in string.punctuation:
            description = description.replace(ch, "")
        desc_words = description.split(' ')
        property_type = 'apartment'
        for j in desc_words:
            for i in types:
                if j == i:
                    property_type = i
                    break

        # Parking
        labels_array = response.css('#tabeight .label::text').extract()
        labels_string=" ".join(labels_array).replace('\n', ' ')
        parking = None
        if 'Stellplatzart' in labels_string or 'Stellplatzart' in description:
            parking = True

        # Furnished
        furnished=None
        if len(labels_array)>0:
            furnished=True



        # Pets
        pets_allowed = None
        for ch in string.punctuation:
            miscellaneous = miscellaneous.replace(ch, "")
        desc_words = miscellaneous.split(' ')
        for j in desc_words:
            if j.lower() == 'haustiere':
                pets_allowed = False
                break

        # Terrace
        labels_array = response.css('#tabeight .label::text').extract()
        labels_string=" ".join(labels_array).replace('\n', ' ')
        terrace = None
        if 'terrasse' in labels_string or 'terrasse' in description:
            terrace = True

        # Balcony
        labels_array = response.css('#tabeight .label::text').extract()
        labels_string = " ".join(labels_array).replace('\n', ' ')
        balcony = None
        if 'balkon' in labels_string:
            balcony = True

        # landlord name
        landlord_name = 'Dr. Jens Butscher'

        # landlord number
        contact_info = response.css('.footer-left p::text').extract()
        landlord_number = contact_info[2].replace('T. ', '')

        # landlord email
        landlord_email = response.css('.footer-left a::text')[0].extract()



        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)# Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        #item_loader.add_value("latitude", latitude) # String
        #item_loader.add_value("longitude", longitude) # String
        #item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) #Array
        item_loader.add_value("external_images_count", len(images)) #Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1

        yield item_loader.load_item()
