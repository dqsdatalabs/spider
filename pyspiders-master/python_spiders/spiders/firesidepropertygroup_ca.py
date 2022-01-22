# -*- coding: utf-8 -*-
# Author: Abdulrahman Abbas

import scrapy

from ..helper import extract_location_from_address, extract_location_from_coordinates, string_found, extract_number_only
from ..loaders import ListingLoader


class FiresidepropertygroupCaSpider(scrapy.Spider):
    name = "firesidepropertygroup_ca"
    start_urls = [
        'https://www.firesidepropertygroup.com/search-property-result/?location=&status=forrent&bedrooms=0&bathrooms=0&keyword=&min_price=0&max_price=2000']
    country = 'canada'  # Fill in the Country's name
    locale = 'ca'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for item in response.xpath('//article'):
            url = item.xpath('.//div[@class="property-featured"]//a[@class="content-thumb"]/@href').get()
            bathroom_count = item.xpath('.//div[@class="bathrooms"]//text()').get()
            room_count = item.xpath('.//div[@class="bedrooms"]//text()').get()
            square_meters = item.xpath('.//div[@class="area"]//text()').get()
            rent = item.xpath('.//div[@class="property-price"]/span/span//text()').get()
            yield scrapy.Request(url, callback=self.populate_item,
                                 meta={"square_meters": square_meters,
                                       "bathroom_count": bathroom_count,
                                       'room_count': room_count,
                                       'rent': rent})

        next_page = response.xpath('//a[@class="next page-numbers"]/@href').get()
        if next_page is not None:
            yield scrapy.Request(next_page, callback=self.parse)

    # 3. SCRAPING level 3
    def populate_item(self, response):

        title = response.xpath('//h1[@class="property-title"]/text()').get()
        location = response.xpath('//h1[@class="property-title"]/small/text()').get()
        longitude, latitude = extract_location_from_address(location)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        info = "".join(response.xpath('//div[@class="property-content"]/p/text()').getall())
        images = response.xpath('//img[@class="attachment-property-image size-property-image"]/@src').getall()

        description = info
        if "At Fireside" in info:
            description = info.rsplit("At ", 1)[0]

        property_type = response.xpath('//span[@class="col-sm-7 detail-field-value type-value"]//text()').get()
        if 'Studio' in property_type:
            property_type = 'Studio'

        details = response.xpath('//div[@class="property-feature-content"]//text()').getall()
        amenities = " ".join(details)
        bathroom = response.meta["bathroom_count"]
        bathroom_count = 1

        if bathroom is not None and ".5" in bathroom:
            bathroom_count = float(bathroom)
            bathroom_count += 0.5

        room_count = response.meta["room_count"]
        if room_count is None:
            room_count = 1

        terrace = False
        if string_found(['terrace', 'terraces'], amenities):
            terrace = True

        elevator = False
        if string_found(['elevator'], amenities):
            elevator = True

        parking = False
        if string_found(['parking'], amenities):
            parking = True

        dishwasher = False
        if string_found(['dishwasher'], amenities):
            dishwasher = True

        washing_machine = False
        if string_found(['Laundry'], amenities):
            washing_machine = True

        balcony = False
        if string_found(['Balcony'], amenities):
            balcony = True

        pets_allowed = False
        if string_found(['Pets Allowed'], amenities):
            pets_allowed = True

        if response.meta["rent"] is not None:
            # MetaData

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)  # String
            item_loader.add_value("external_source", self.external_source)  # String

            # item_loader.add_value("external_id", external_id) # String
            item_loader.add_value("position", self.position)  # Int
            item_loader.add_value("title", title)  # String
            item_loader.add_value("description", description)  # String

            # # Property Details
            item_loader.add_value("city", city)  # String
            item_loader.add_value("zipcode", zipcode)  # String
            item_loader.add_value("address", address)  # String
            item_loader.add_value("latitude", str(latitude))  # String
            item_loader.add_value("longitude", str(longitude))  # String
            # item_loader.add_value("floor", floor) # String
            item_loader.add_value("property_type", property_type.lower())  # String
            item_loader.add_value("square_meters", response.meta["square_meters"])  # Int
            item_loader.add_value("room_count", room_count)  # Int
            item_loader.add_value("bathroom_count", int(bathroom_count))  # Int

            # item_loader.add_value("available_date", available_date) # String => date_format

            item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
            # item_loader.add_value("furnished", furnished) # Boolean
            item_loader.add_value("parking", parking)  # Boolean
            item_loader.add_value("elevator", elevator)  # Boolean
            item_loader.add_value("balcony", balcony)  # Boolean
            item_loader.add_value("terrace", terrace)  # Boolean
            # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
            item_loader.add_value("washing_machine", washing_machine)  # Boolean
            item_loader.add_value("dishwasher", dishwasher)  # Boolean

            # # Images

            item_loader.add_value("images", images)  # Array
            item_loader.add_value("external_images_count", len(images))  # Int

            # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

            # Monetary Status
            item_loader.add_value("rent", response.meta["rent"].replace(',', '.'))  # Int
            # item_loader.add_value("deposit", deposit) # Int
            # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
            # item_loader.add_value("utilities", utilities) # Int
            item_loader.add_value("currency", "CAD")  # String

            # item_loader.add_value("water_cost", water_cost) # Int
            # item_loader.add_value("heating_cost", heating_cost) # Int

            # item_loader.add_value("energy_label", energy_label) # String

            # # LandLord Details
            item_loader.add_value("landlord_name", "Fireside Property Group Ltd.")  # String
            item_loader.add_value("landlord_phone", "403-228-4303")  # String
            item_loader.add_value("landlord_email", 'info@firesidepropertygroup.com')  # String

            self.position += 1
            yield item_loader.load_item()

        else:
            items = response.xpath('//div[@class="property-content"]//h3//text()').getall()
            for item in range(len(items)):
                item_loader = ListingLoader(response=response)

                room_count = extract_number_only(items[item].split("$")[0])
                if room_count == 0:
                    room_count = 1

                rent = items[item].split("$")[1]

                item_loader.add_value("external_link", response.url)  # String
                item_loader.add_value("external_source", self.external_source)  # String

                # item_loader.add_value("external_id", external_id) # String
                item_loader.add_value("position", self.position)  # Int
                item_loader.add_value("title", title)  # String
                item_loader.add_value("description", description)  # String

                # # Property Details
                item_loader.add_value("city", city)  # String
                item_loader.add_value("zipcode", zipcode)  # String
                item_loader.add_value("address", address)  # String
                item_loader.add_value("latitude", str(latitude))  # String
                item_loader.add_value("longitude", str(longitude))  # String
                # item_loader.add_value("floor", floor) # String
                item_loader.add_value("property_type", property_type.lower())  # String
                item_loader.add_value("square_meters", response.meta["square_meters"])  # Int
                item_loader.add_value("room_count", room_count)  # Int
                item_loader.add_value("bathroom_count", int(bathroom_count))  # Int

                # item_loader.add_value("available_date", available_date) # String => date_format

                item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
                # item_loader.add_value("furnished", furnished) # Boolean
                item_loader.add_value("parking", parking)  # Boolean
                item_loader.add_value("elevator", elevator)  # Boolean
                item_loader.add_value("balcony", balcony)  # Boolean
                item_loader.add_value("terrace", terrace)  # Boolean
                # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
                item_loader.add_value("washing_machine", washing_machine)  # Boolean
                item_loader.add_value("dishwasher", dishwasher)  # Boolean

                # # Images

                item_loader.add_value("images", images)  # Array
                item_loader.add_value("external_images_count", len(images))  # Int

                # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

                # Monetary Status
                item_loader.add_value("rent", rent)  # Int
                # item_loader.add_value("deposit", deposit) # Int
                # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
                # item_loader.add_value("utilities", utilities) # Int
                item_loader.add_value("currency", "CAD")  # String

                # item_loader.add_value("water_cost", water_cost) # Int
                # item_loader.add_value("heating_cost", heating_cost) # Int

                # item_loader.add_value("energy_label", energy_label) # String

                # # LandLord Details
                item_loader.add_value("landlord_name", "Fireside Property Group Ltd.")  # String
                item_loader.add_value("landlord_phone", "403-228-4303")  # String
                item_loader.add_value("landlord_email", 'info@firesidepropertygroup.com')  # String

                self.position += 1
                yield item_loader.load_item()
