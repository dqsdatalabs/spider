# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, extract_location_from_address


class TricorprealtyPyspiderCanadaSpider(scrapy.Spider):
    name = "TricorpRealty"
    start_urls = ['https://www.tricorprealty.com/for-rent']
    allowed_domains = ["tricorprealty.com"]
    country = 'canada' # Fill in the Country's name
    locale = 'en' # Fill in the Country's locale, look up the docs if unsure
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
        apartments_divs = response.css('div#Containerru8pc div._1Q9if')[1:]
        images_div = response.css('div._3jQ8z')
        i = 0
        for apartment_div in apartments_divs:
            images_all = images_div[i].css('wix-image._1-6YJ._3rIAj::attr(data-image-info)').extract()
            images_raw = []
            images = []
            for item in images_all:
                item = item.split('"uri":"')[1]
                item = item.split('","displayMode')[0]
                images_raw.append(item)
            images_raw = images_raw
            for image in images_raw:
                image = "https://static.wixstatic.com/media/" + image
                images.append(image)

            city = apartment_div.xpath('.//h6//span[1]/text()').extract()
            if len(city[0]) < 2:
                city = apartment_div.xpath('.//p//span//span//span//span//span/text()').extract()
                if len(city) < 1:
                    city = apartment_div.xpath('.//p//span//span/text()').extract()
            city = city[0]
            if "-" in city:
                city = city.split("-")[1]
                city = city.strip()

            title_basic = apartment_div.xpath('.//h6//span[2]//span//span/text()').extract()
            if len(title_basic) < 1:
                title_basic = apartment_div.xpath('.//h6[2]//span//span//span/text()').extract()
            if "reat location" in title_basic[0]:
                title_basic = apartment_div.xpath('.//h6[2]//span//span//span//span/text()').extract()
            title_basic = title_basic[0]

            landlord_info = apartment_div.css('span.color_23 span span::text').extract()
            landlord_number = None
            for item in landlord_info:
                if "call" in item:
                    landlord_number = item
            landlord_number = landlord_number.split("at")[1]
            landlord_number = landlord_number.replace(".", "")
            landlord_number = landlord_number.strip()
            landlord_email = "management@tricorprealty.com"
            landlord_name = "Tricorp Realty"

            utilities_one = apartment_div.xpath('.//h6//span/text()').extract()
            utilities_two = apartment_div.xpath('.//h6/text()').extract()
            utilities = utilities_one + utilities_two

            description_raw = []
            for desc in utilities:
                if "call" not in desc.lower():
                    if "video" not in desc.lower():
                        if "$" not in desc.lower():
                            if "jan" not in desc.lower():
                                if "feb" not in desc.lower():
                                    if title_basic not in desc:
                                        description_raw.append(desc.strip())
            description = " ".join(description_raw)
            description = description.replace("  ", " ")
            description = description.replace("\u200b", "")

            balcony = None
            parking = None
            units = []
            for item in utilities:
                if "balcon" in item.lower():
                    balcony = True
                if "parking" in item.lower():
                    parking = True
                if "$" in item:
                    units.append(item)
            i += 1
            for unit in units:
                item_loader = ListingLoader(response=response)
                unit = unit.split(":")
                title_remain = unit[0]
                title = title_basic + " | " + title_remain

                address = title_basic.split(",")[1]
                address = address.strip()
                address = address + ", " + city + ", Canada"
                longitude, latitude = extract_location_from_address(address)
                longitude = str(longitude)
                latitude = str(latitude)
                zipcode, no_city, no_address = extract_location_from_coordinates(longitude, latitude)

                room_count = unit[0]
                room_count = room_count.split()[0]
                room_count = int(room_count)

                rent = unit[1]
                rent = rent.replace("$", "")
                rent = float(rent)
                rent = round(rent)
                rent = int(rent)

                url_title = title_basic + " " + title_remain
                url_title = url_title.replace("  ", " ")
                url_title = url_title.replace(" ", "-")
                external_link = response.url + "#" + url_title

                property_type = "apartment"

                item_loader.add_value("external_link", external_link) # String
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
                #item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
                #item_loader.add_value("square_meters", square_meters) # Int
                item_loader.add_value("room_count", room_count) # Int
                #item_loader.add_value("bathroom_count", bathroom_count) # Int

                #item_loader.add_value("available_date", available_date) # String => date_format

                #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
                #item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking) # Boolean
                #item_loader.add_value("elevator", elevator) # Boolean
                item_loader.add_value("balcony", balcony) # Boolean
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
                item_loader.add_value("currency", "CAD") # String

                #item_loader.add_value("water_cost", water_cost) # Int
                #item_loader.add_value("heating_cost", heating_cost) # Int

                #item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", landlord_name) # String
                item_loader.add_value("landlord_phone", landlord_number) # String
                item_loader.add_value("landlord_email", landlord_email) # String

                self.position += 1
                yield item_loader.load_item()
