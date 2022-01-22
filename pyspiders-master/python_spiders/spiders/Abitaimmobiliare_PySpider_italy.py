# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, extract_location_from_coordinates


class AbitaimmobiliarePyspiderItalySpider(scrapy.Spider):
    name = "abitaimmobiliare"
    start_urls = ['https://www.abitaimmobiliare.com/annunci/affitto-case/?tipo=appartamenti%2Cville-e-case-unifamiliari&cat_speciale&comune&zona',
                  'https://www.abitaimmobiliare.com/annunci/affitto-case/page/2/?tipo=appartamenti%2Cville-e-case-unifamiliari&cat_speciale&comune&zona']
    allowed_domains = ["abitaimmobiliare.com"]
    country = 'italy'  # Fill in the Country's name
    locale = 'it'  # Fill in the Country's locale, look up the docs if unsure
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
        # Your code goes here
        main_div = response.xpath('.//div[contains(@id, "content")]//section[contains(@class, "col-md-9")]')
        # next_page = main_div.xpath('.//nav[contains(@class, "col-12 text-center")]/@href').extract()
        apartments_div = main_div.xpath('.//article[contains(@class, "anteprimaimmobile--list")]//div[contains(@class, "row")]')
        for apartment_div in apartments_div:
            apartment_info = apartment_div.xpath('.//div[contains(@class, "anteprimaimmobile--text")]')
            apartment_url = apartment_div.xpath('.//a/@href').extract()[0]
            external_id = apartment_info.xpath('.//small[contains(@class, "rif")]/text()').extract()
            title = apartment_info.xpath('.//h3//a/text()').extract()
            yield scrapy.Request(url=apartment_url, callback=self.populate_item, meta={"external_id": external_id, "title": title})

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
        title = response.meta.get("title")[0]
        property_type_it = (title.split())[0]
        property_type = None
        if property_type_it == "Appartamento":
            property_type = "apartment"
        elif property_type_it == "Unifamiliare":
            property_type = "house"
        external_id = response.meta.get("external_id")
        apartment_info_div = response.xpath('.//div[contains(@class, "mainImmobile")]//article')
        images = apartment_info_div.xpath('.//div[contains(@class, "immobile-gallery")]//a[contains(@rel, "galleria")]/@href').extract()
        floor_plan_images = apartment_info_div.xpath('.//nav[contains(@id, "iconBar")]//a[contains(@rel, "galleria")]/@href').extract()
        square_meters_wrong = apartment_info_div.xpath('.//h2/text()')[0].extract()
        square_meters = int(square_meters_wrong.replace(" m", ""))
        description = apartment_info_div.xpath('.//p/text()').extract()
        try:
            locationOfInfo = str(description).index(" Per Info")
            description = str(description)[:locationOfInfo]
        except:
            pass

        location = response.css("meta[name='geo.position']::attr(content)").extract()
        if len(location) > 0:
            latitude, longitude = location[0].split(";")
        else:
            second_address = apartment_info_div.xpath('normalize-space(.//h1//span/text())').extract()
            address = title + second_address[0]
            lon, lat = extract_location_from_address(address)
            latitude = str(lat)
            longitude = str(lon)
        zipcode, city, address = extract_location_from_coordinates(str(longitude), str(latitude))

        rent = apartment_info_div.xpath('.//span[contains(@class, "priceBlock")]//span[contains(@class, "price")]/text()').extract()
        rent = int(rent[0])
        all_rooms = apartment_info_div.xpath('.//div[contains(@class, "shortInfo")]//span/text()')[1:].extract()
        key = all_rooms[1::2]
        value = all_rooms[0::2]
        rooms_dict = dict(zip(key, value))
        room_count = 0
        bathroom_count = 0
        if rooms_dict.get(' camere'):
            room_count = rooms_dict[' camere']
        if rooms_dict.get(' camera'):
            room_count = rooms_dict[' camera']
        if rooms_dict.get(' bagno'):
            bathroom_count = rooms_dict[' bagno']

        apartment_table = response.xpath('.//div[contains(@class, "row")]//ul[contains(@id, "tabellaImmobile")]//li')
        t_key = []
        t_value = []
        for items in apartment_table:
            t_key.append(items.xpath('div[1]/text()').extract_first())
            t_value.append(items.xpath('div[2]/text()').extract_first())
        apartment_dict = dict(zip(t_key, t_value))
        energy_label = None
        if apartment_dict.get('Classe energetica'):
            energy_label = apartment_dict['Classe energetica']
        floor = None
        if apartment_dict.get('Piano'):
            floor = apartment_dict['Piano']
        balcony = None
        if apartment_dict.get('Balcone'):
            balcony = True
        furnished = None
        if apartment_dict.get('Arredamento'):
            furnished = True
        parking = None
        if apartment_dict.get('Posto auto'):
            parking = True
        terrace = None
        if apartment_dict.get('Numero terrazzi'):
            terrace = True

        landlord_name = "Abita Immobiliare"
        landlord_number = response.xpath('.//address//div[contains(@class, "telefono")]//strong/text()').extract()
        landlord_email = response.xpath('.//address/text()')[1].extract()




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
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        #item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        # item_loader.add_value("deposit", rent) # Int
        # item_loader.add_value("prepaid_rent", rent) # Int
        # item_loader.add_value("utilities", rent) # Int
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
