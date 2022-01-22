# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas
import scrapy

from ..helper import extract_location_from_address, extract_number_only, string_found, remove_white_spaces
from ..loaders import ListingLoader


class EdisonpropertiesCaSpider(scrapy.Spider):
    name = "edisonproperties_ca"
    start_urls = [
        'https://www.edisonproperties.ca/searchlisting.aspx'

    ]
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
        for item in response.xpath('//div[@id="resultBody"]/div'):
            links_level1 = item.xpath('.//div[@class="span11 prop-heading"]//a[2]/@href').get()
            company_name = links_level1.split("/")[-2]
            address = response.xpath('//div[@class="prop-address"]//span[@class="propertyAddress"]//text()').get()
            city = response.xpath('//div[@class="prop-address"]//span[@class="propertyCity"]//text()').get()
            zipcode = response.xpath('//div[@class="prop-address"]//span[@class="propertyZipCode"]//text()').get()

            yield scrapy.Request(url=links_level1, callback=self.get_description,
                                 meta=
                                 {
                                     "company_name": company_name,
                                     'address': address,
                                     'city': city,
                                     "zipcode": zipcode,
                                 })

    def get_description(self, response):

        description = response.xpath('//div[@id="ctmcontentcontainer"]//text()').getall()
        yield scrapy.Request(f"https://www.edisonproperties.ca/{response.meta['company_name']}/photogallery",
                             callback=self.get_images,
                             meta=
                             {
                                 "description": description,
                                 "landlord_name": response.meta['company_name'],
                                 'address': response.meta['address'],
                                 'city': response.meta['city'],
                                 "zipcode": response.meta['zipcode'],
                                 "company_name": response.meta["company_name"],
                             })

    def get_images(self, response):

        images = response.xpath('//div[@class="item"]//img//@src').getall()
        yield scrapy.Request(f"https://www.edisonproperties.ca/{response.meta['company_name']}/amenities",
                             callback=self.get_amenities,
                             meta=
                             {
                                 "images": images,
                                 "landlord_name": response.meta['company_name'],
                                 'address': response.meta['address'],
                                 'city': response.meta['city'],
                                 "zipcode": response.meta['zipcode'],
                                 "company_name": response.meta["company_name"],
                                 "description": response.meta["description"],

                             })

    def get_amenities(self, response):

        amenities = " ".join(response.xpath('//div[@id="amenities-layout-new"]//text()').getall())
        yield scrapy.Request(f"https://www.edisonproperties.ca/{response.meta['company_name']}/floorplans",
                             callback=self.follow_pages,
                             meta=
                             {
                                 "amenities": amenities,
                                 "images": response.meta["images"],
                                 "landlord_name": response.meta['company_name'],
                                 'address': response.meta['address'],
                                 'city': response.meta['city'],
                                 "zipcode": response.meta['zipcode'],
                                 "company_name": response.meta["company_name"],
                                 "description": response.meta["description"],

                             })

    def follow_pages(self, response):
        links_data = \
            response.xpath('//script[59]//text()').get().replace("floorplans:", "!").replace("propertyID:", "!").split(
                "!")[
                -2].split("Frequency:")
        for item in links_data:
            if "availableCount: 0" in item:
                links_data.remove(item)

        links_data = "".join(links_data).split(",")
        for item in links_data:
            if "availableUnitsURL" in item:
                url = item.split("Apartments&myOle")[-1].replace("';", "").replace('"', "")
                flink = f"https://www.edisonproperties.ca/{response.meta['company_name']}/availableunits.aspx?myOle{url}"
                yield scrapy.Request(url=flink, callback=self.populate_item,
                                     meta=
                                     {
                                         "landlord_name": response.meta['company_name'],
                                         "flink": flink,
                                         'address': response.meta['address'],
                                         'city': response.meta['city'],
                                         "zipcode": response.meta['zipcode'],
                                         "images": response.meta["images"],
                                         "amenities": response.meta["amenities"],
                                         "description": response.meta["description"],
                                     })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        amenities = response.meta["amenities"]
        images = response.meta["images"]
        landlord_name = response.meta['landlord_name']
        location = response.meta['address']
        city = response.meta['city']
        zipcode = response.meta['zipcode']
        address = city + ", " + location + ", " + zipcode
        longitude, latitude = extract_location_from_address(address)
        description = "".join(response.meta["description"])

        room = response.xpath('//div[@id="other-floorplans"]//h3//text()').get().split("-")[-1].split(",")
        title = remove_white_spaces(
            response.xpath('//div[@id="other-floorplans"]//h3//text()').get().replace("Floor Plan", ""))
        ftitle = landlord_name + ":" + " ".join(title)

        bathroom_count = extract_number_only(room[-1])
        room_count = extract_number_only(room[0])

        if "0" in room_count or room_count == 0:
            room_count = 1

        landlord_number = response.xpath('//span[@class="click_to_call_foot"]//text()').get()

        floor_plan_images = response.xpath('//span[@class="view-floor-plan"]//a//@onmouseout').get().split(',')[
            -2].replace("'", '')

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
        for item in response.xpath('//tbody//tr'):
            rent = item.xpath('.//td[@data-label="Rent"]//text()').get().replace(",", '.')
            square_meters = item.xpath('.//td[@data-label="Sq. Ft."]//text()').get()
            external_id = item.xpath('.//td[@data-label="Apartment"]//text()').get()

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", f"{response.url}/{external_id}")  # String
            item_loader.add_value("external_source", self.external_source)  # String

            item_loader.add_value("external_id", external_id)  # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", ftitle)  # String
            item_loader.add_value("description", description)  # String

            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", str(latitude))  # String
            item_loader.add_value("longitude", str(longitude))  # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", "apartment")  # String
            item_loader.add_value("square_meters", square_meters)  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", bathroom_count)  # Int

            # item_loader.add_value("available_date", available_date) # String => date_format

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
            item_loader.add_value("rent", rent)  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name)  # String
            item_loader.add_value("landlord_phone", landlord_number)  # String
            # item_loader.add_value("landlord_email", landlord_email) # String

            self.position += 1
            yield item_loader.load_item()
