# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy

from ..helper import extract_location_from_address, string_found, extract_location_from_coordinates, remove_unicode_char
from ..loaders import ListingLoader



class BglDeSpider(scrapy.Spider):
    name = "bgl_de"
    start_urls = ["https://www.bgl.de/vermietung"]
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
        apartment_page_links = response.xpath('//div[@class="property-item"]//a')
        yield from response.follow_all(apartment_page_links, self.populate_item)

        pages = response.xpath('//ul[@class="pagination"]//li')
        for page in range(len(pages) // 2):
            next_page = f"https://www.bgl.de/vermietung?address=&property_type=3&nroom=&sqft_min=&sqft_max=&max_price=&type_aufzug=LIKE&type_balkon=LIKE&advfieldLists=14%2C15%2C16%2C17&sortby=a.price&orderby=asc&currency_item=&live_site=https%3A%2F%2Fwww.bgl.de%2F&limitstart={page + 2}0&process_element=&option=com_osproperty&task=property_advsearch&show_more_div=0&Itemid=639&search_param=type%3A3_type%3A3_country%3A71_min_price%3A100_sortby%3Aa.price_orderby%3Aasc&list_id=0&adv_type=3&show_advancesearchform=1&advtype_id_1=&advtype_id_2=&advtype_id_3=15%2C17%2C16%2C14&advtype_id_4=&advtype_id_5=&advtype_id_6=&advtype_id_7=18%2C14%2C19%2C20%2C21%2C22%2C23"
            yield scrapy.Request(next_page, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        location = response.xpath('//h3[@class="adresse"]//text()').get()
        longitude, latitude = extract_location_from_address(location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        title = response.xpath('//h1[@class="headline h2"]//text()').get()
        square_meters = response.xpath('//div[@class="groesse weiss pict"]//text()').get()
        rent = response.xpath('//div[@class="miete weiss pict"]//text()').get()
        pro_details = response.xpath('//div[@class="infoblock col-xs-12"]//ul//text()').getall()
        energy_label = response.xpath('//div[@class="knob"]//text()').get()
        description = response.xpath('//div[@class="col-sm-8"]//p[1]//text()').get()
        details1 = response.xpath('//p[@class="ausstattung"]//text()').getall()
        amenities = " ".join(details1)

        external_id = None
        if "Objektnummer" in pro_details:
            position = pro_details.index("Objektnummer")
            external_id = pro_details[position + 2]

        room_count = None
        if "Zimmer" in pro_details:
            position = pro_details.index("Zimmer")
            room_count = pro_details[position + 2]

        floor = None
        if "Etage" in pro_details:
            position = pro_details.index("Etage")
            floor = pro_details[position + 2]

        utilities = None
        if "Nebenkosten" in pro_details:
            position = pro_details.index("Nebenkosten")
            utilities = pro_details[position + 2]

        heating_cost = None
        if "inkl. Heizkosten" in pro_details:
            position = pro_details.index("inkl. Heizkosten")
            heating_cost = pro_details[position + 2]

        bathroom_count = 1
        if string_found(['WC', 'Gäste WC'], amenities):
            bathroom_count = 2

        balcony = False
        if string_found(['Balkone','Balkon' ], amenities):
            balcony = True
        elevator = False
        if string_found(['Aufzug', 'Aufzügen'], amenities):
            elevator = True

        parking = False
        if string_found(['Stellplatz', 'Garage', 'Tiefgaragenstellplätze'], amenities):
            parking = True

        terrace = False
        if string_found(['Terrasse'], amenities):
            terrace = True

        swimming_pool = False
        if string_found(['Aufzug'], amenities):
            swimming_pool = True

        pets_allowed = False
        if string_found(['Haustierhaltung ist gestattet'], amenities):
            pets_allowed = True

        images = []
        floor_plan_images = []
        all_images = response.xpath('//div[@id="slides"]//a//@href').getall()
        for image in all_images:
            if "grundriss" in image:
                floor_plan_images.append(image)
            else:
                images.append(image)

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", remove_unicode_char(title))  # String
        item_loader.add_value("description", remove_unicode_char(description))  # String

        # # Property Details
        item_loader.add_value("city", remove_unicode_char(city))  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", remove_unicode_char(address))  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type", "apartment")  # String
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        # item_loader.add_value("washing_machine", washing_machine) # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images

        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost)  # Int

        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        landlord_number = response.xpath('//div[@class="kontaktblock col-xs-12"]//p[2]//text()').get()
        item_loader.add_value("landlord_name", "BGL Baugenossenschaft Leipzig eG") # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", "wohnungsanfragen@bgl.de") # String

        self.position += 1
        yield item_loader.load_item()
