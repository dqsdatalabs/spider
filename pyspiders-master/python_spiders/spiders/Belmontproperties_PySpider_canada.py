# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates, sq_feet_to_meters


class BelmontpropertiesPyspiderCanadaSpider(scrapy.Spider):
    name = "Belmontproperties"
    start_urls = [
        'https://www.belmontproperties.ca/?search-listings=true',
        'https://www.belmontproperties.ca/page/2/?search-listings=true'
    ]
    allowed_domains = ["belmontproperties.ca"]
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
        # Your code goes here
        apartments_divs = response.xpath('.//div[contains(@id, "listings-results")]//li[contains(@class, "listing")]')
        for apartment_div in apartments_divs:
            availability = apartment_div.xpath('.//figure//h6[contains(@class, "available")]//span')
            if availability:
                apartment_url = apartment_div.xpath('.//figure//a[contains(@class, "listing-featured-image")]/@href')[0].extract()
                title = apartment_div.xpath('.//div[contains(@class, "grid-listing-info")]//header//h5//a/text()')[0].extract()
                location = apartment_div.xpath('.//div[contains(@class, "grid-listing-info")]//header//p/text()')[0].extract()
                address = title + ", " + location
                yield scrapy.Request(url=apartment_url, callback=self.populate_item, meta={"title": title, "address": address})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        apartment_info = response.xpath('.//div[contains(@class, "container")]//article')
        suites = apartment_info.xpath('.//div[contains(@class, "post-content")]//div[contains(@id, "listing-plans")]//table//tr')
        i = 1
        for suite in suites:
            item_loader = ListingLoader(response=response)

            external_link = response.url + "#unit" + str(i)

            room_count = suite.xpath('.//td[2]/text()')[0].extract()
            room_count = float(room_count)
            room_count = round(room_count)
            room_count = int(room_count)

            bathroom_count = suite.xpath('.//td[3]/text()')[0].extract()
            bathroom_count = float(bathroom_count)
            bathroom_count = round(bathroom_count)
            bathroom_count = int(bathroom_count)

            sqm_feet = suite.xpath('.//td[4]/text()')[0].extract()
            square_meters = int(sqm_feet)
            # square_meters = sq_feet_to_meters(sqm_feet)

            rent = suite.xpath('.//td[5]/text()')[0].extract()
            rent = rent.replace("$", "")
            rent = int(rent)

            # 'Available January 15th, 2022
            available_date_str = suite.xpath('.//td[6]/text()')[0].extract()
            available_date_str = available_date_str.replace("Available", "")
            days = []
            if "Now" in available_date_str:
                available_date = None
            else:
                available_date_str = available_date_str.split()
                month = available_date_str[0]
                month = self.convertMonthToNum(month)
                day = available_date_str[1]
                for x in day:
                    if x.isdigit():
                        days.append(x)
                day = "".join(days)
                if len(day) == 1:
                    day = "0" + day
                year = available_date_str[2]
                available_date = year + "-" + month + "-" + day

            title = response.meta.get("title")
            title = title + " - " + str(room_count) + " Bedroom"
            address = response.meta.get("address")

            images = response.xpath('.//a[contains(@class, "gallery-item")]/@href').extract()

            balcony = None
            parking = None
            swimming_pool = None

            property_type = "apartment"

            if room_count == 0:
                room_count = 1
                property_type = "studio"

            description_all = apartment_info.xpath('.//div[contains(@class, "post-content")]//div[contains(@id, "listing-content")]//p/text()').extract()
            landlord_name = None
            description = None
            for item in description_all:
                if "Parking" in item:
                    parking = True
                if "Manager:" in item:
                    what_to_remove = description_all.index(item)
                    description = description_all[:what_to_remove]
                    landlord_info = description_all[what_to_remove:]
                    landlord_name = landlord_info[0].split(":")
                    landlord_name = landlord_name[1]
                    landlord_name = landlord_name.replace("– Tel", "")
                if "Managers:" in item:
                    what_to_remove = description_all.index(item)
                    description = description_all[:what_to_remove]
                    landlord_info = description_all[what_to_remove:]
                    landlord_name = landlord_info[0].split(":")
                    landlord_name = landlord_name[1]
                    landlord_name = landlord_name.replace("– Tel", "")
                if not description:
                    description = description_all

            if landlord_name == None:
                landlord_name = "Belmont Properties"

            tel_email = apartment_info.xpath('.//div[contains(@class, "post-content")]//div[contains(@id, "listing-content")]//p//a/@href').extract()
            landlord_email = None
            landlord_number = None
            if tel_email:
                for item in tel_email:
                    if item.startswith("mailto"):
                        landlord_email = item.replace("mailto:", "")
                    if item.startswith("tel"):
                        landlord_number = item.replace("tel:", "")

            lat_lng = response.xpath('.//script').extract()
            longitude = None
            city = None
            zipcode = None
            latitude = None
            for item in lat_lng:
                if "LatLng" in item:
                    lat_lng_find = lat_lng[lat_lng.index(item)]
                    lat_lng_find = lat_lng_find.split("LatLng")
                    lat_lng_find = lat_lng_find[1]
                    lat_lng_find = lat_lng_find.split(")")
                    lat_lng_find = lat_lng_find[0]
                    lat_lng_find = lat_lng_find.replace("(", "")
                    lat_lng_find = lat_lng_find.split(",")
                    latitude = str(lat_lng_find[0])
                    longitude = str(lat_lng_find[1])
            if latitude:
                zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

            amenities = apartment_info.xpath('.//div[contains(@class, "post-content")]//div[contains(@id, "listing-features")]//ul//li/text()').extract()
            for amenity in amenities:
                if "Balcony" in amenity:
                    balcony = True
                if "Parking" in amenity:
                    parking = True
                if "Swimming Pool" in amenity:
                    swimming_pool = True

            # # MetaData
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
            item_loader.add_value("square_meters", square_meters) # Int
            item_loader.add_value("room_count", room_count) # Int
            item_loader.add_value("bathroom_count", bathroom_count) # Int

            item_loader.add_value("available_date", available_date) # String => date_format

            #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
            #item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking) # Boolean
            #item_loader.add_value("elevator", elevator) # Boolean
            item_loader.add_value("balcony", balcony) # Boolean
            #item_loader.add_value("terrace", terrace) # Boolean
            item_loader.add_value("swimming_pool", swimming_pool) # Boolean
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

            i += 1

            self.position += 1
            yield item_loader.load_item()

    def convertMonthToNum(self, monthName):
        months = {
            "january": "01",
            "february": "02",
            "march": "03",
            "april": "04",
            "may": "05",
            "june": "06",
            "july": "07",
            "august": "08",
            "september": "09",
            "october": "10",
            "november": "11",
            "december": "12",
        }
        return months.get(str(monthName).lower())