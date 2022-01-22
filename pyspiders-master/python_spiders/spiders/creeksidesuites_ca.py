# -*- coding: utf-8 -*-
# Author: A.Abbas
import scrapy

from ..helper import *
from ..loaders import ListingLoader


class CreeksidesuitesCaSpider(scrapy.Spider):
    name = "creeksidesuites_ca"
    start_urls = ['https://www.creeksidesuites.ca']
    country = 'canada'  # Fill in the Country's name
    locale = 'en'  # Fill in the Country's locale, look up the docs if unsure
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
        description = response.xpath('//div[@id="InnerContentDiv"]//b//text()').get()
        yield scrapy.Request("https://www.creeksidesuites.ca/apartments/on/kingston/photogallery",
                             callback=self.get_images, meta={"description": description})

    def get_images(self, response, **kwargs):
        images = response.xpath('//img[@class=" lazy "]//@data-src').getall()
        yield scrapy.Request("https://www.creeksidesuites.ca/apartments/on/kingston/amenities.aspx",
                             callback=self.get_amenities,
                             meta=
                             {
                                 "description": response.meta["description"],
                                 "images": images,
                             }
                             )

    def get_amenities(self, response):
        amenities = " ".join(response.xpath('//div[@class="amenities"]//text()').getall())
        yield scrapy.Request("https://www.creeksidesuites.ca/apartments/on/kingston/mapsanddirections.aspx",
                             callback=self.get_location,
                             meta=
                             {
                                 "amenities": amenities,
                                 "images": response.meta["images"],
                                 "description": response.meta["description"],
                             })

    def get_location(self, response):
        latitude, longitude = response.xpath('//span[@class="map-views"]/button/@onclick').get().split("lat")[
            -1].replace("=", "").split("&lng")
        yield scrapy.Request("https://www.creeksidesuites.ca/apartments/on/kingston/availableunits",
                             callback=self.get_adds,
                             meta=
                             {
                                 "amenities": response.meta["amenities"],
                                 "images": response.meta["images"],
                                 "latitude": latitude,
                                 "longitude": longitude,
                                 "description": response.meta["description"],
                             })

    def get_adds(self, response):
        global title, floor_plan_images, room_count, bathroom_count
        father = len(response.xpath('//div[@id="innerformdiv"]/div[@class="row-fluid"]'))

        for ad in range(1, father):
            if ad % 2 != 0:
                title = response.xpath(f'//div[@id="innerformdiv"]//div[{ad}]//h3//text()').get()
                floor_plan_images = \
                response.xpath(f'//div[@id="innerformdiv"]//div[{ad}]//div[@id="links"]//a//@onmouseout').get().split(
                    '?')[0].split(",'")[-1]
                bathroom_count = title.split("-")[-1].split(",")[-1]
                room_count = title.split("-")[-1].split(",")[0]

            if ad % 2 == 0:

                links = response.xpath(f'//div[@id="innerformdiv"]//div[{ad}]//table//tbody//tr')
                for ad_link in links:
                    square_meters = ad_link.xpath('.//td[@data-label="Sq. Ft."]//text()').get()
                    rent = ad_link.xpath('.//td[@data-label="Rent"]//text()').get().replace(",",'.')
                    external_id = ad_link.xpath('.//td[@data-label="Apartment"]//text()').get()
                    url = ad_link.xpath('.//td[@data-label="Action"]//input//@onclick').get().replace("SetTermsUrl('",
                                                                                                      "").replace("')",
                                                                                                                  '')
                    available_date = url.split("Date=")[-1]
                    yield scrapy.Request(
                        "https://www.creeksidesuites.ca/apartments/on/kingston/{}".format(url),
                        callback=self.populate_item,
                        meta=
                        {
                            "rent": rent,
                            "title": title,
                            "room_count": room_count,
                            "bathroom_count": bathroom_count,
                            "floor_plan_images": floor_plan_images,
                            'square_meters': square_meters,
                            "external_id": external_id,
                            'available_date': available_date,
                            "amenities": response.meta["amenities"],
                            "images": response.meta["images"],
                            "latitude": response.meta["latitude"],
                            "longitude": response.meta["longitude"],
                            "description": response.meta["description"],
                        })

    #     # 3. SCRAPING level 3

    def populate_item(self, response):
        amenities = response.meta["amenities"]

        images = response.meta["images"]

        latitude = response.meta["latitude"]
        longitude = response.meta["longitude"].replace("')", '')
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        furnished = False
        if string_found(['Available furnished'], amenities):
            furnished = True

        balcony = False
        if string_found(['Balcony'], amenities):
            balcony = True

        parking = False
        if string_found(['Parking', 'Underground parking'], amenities):
            parking = True

        elevator = False
        if string_found(['elevator', 'elevators'], amenities):
            elevator = True

        terrace = False
        if string_found(['terrace'], amenities):
            terrace = True

        pets_allowed = False
        if string_found(['Pet Friendly', 'Pet-Free Building', 'Pet free property'], amenities):
            pets_allowed = True

        swimming_pool = False
        if string_found(['pool', "swimming pool", 'swimming', 'Indoor pool'], amenities):
            swimming_pool = True

        washing_machine = False
        if string_found(['Laundry', 'Laundry Facilities'], amenities):
            washing_machine = True

        dishwasher = False
        if string_found(['dishwasher'], amenities):
            dishwasher = True

        # # MetaData
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", response.meta['external_id'])  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", response.meta['title'])  # String
        item_loader.add_value("description", response.meta["description"])  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", "apartment")  # String
        item_loader.add_value("square_meters", response.meta['square_meters'])  # Int
        item_loader.add_value("room_count", extract_number_only(response.meta["room_count"]))  # Int
        item_loader.add_value("bathroom_count", extract_number_only(response.meta["bathroom_count"]))  # Int

        item_loader.add_value("available_date", extract_date(response.meta["available_date"]))  # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        item_loader.add_value("floor_plan_images", f"https://cdngeneralcf.rentcafe.com{floor_plan_images}")  # Array

        # # Monetary Status
        item_loader.add_value("rent", response.meta['rent'])  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "CAD")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Creekside')  # String
        item_loader.add_value("landlord_phone", '(877) 942-7335')  # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
