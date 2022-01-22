# -*- coding: utf-8 -*-
# Author: Mahmoud wessam
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_coordinates_regex, extract_location_from_coordinates


class BelongLivingComSpider(scrapy.Spider):
    name = "belong_living_com"
    start_urls = ['https://www.belong-living.com/']
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
        for appartment in response.css("div.featured_car"):
            url = appartment.css("a.car_name").attrib['href']
            yield scrapy.Request(url,
                                 callback=self.populate_item,
                                 )

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        id = response.url.split('/')[-2]

        title = response.css(
            '#content > div.leftSection > div.description.left.sidebarH2 > div > h2::text').get()

        description = response.css(
            '#content > div.leftSection > div.sidebar.widgets.col_04.left.marginTop20 > div > div:nth-child(1) > p::text').extract()

        available_date = response.xpath(
            '//*[@id="content"]/script[3]/text()').get()
        available_date = available_date.split(
            'var moveIn = makeDate("')[1].split('");')[0]

        images = response.css(
            'div.slider_appart::attr(style)').extract()
        for i in range(len(images)):
            images[i] = images[i].split(":url('")[1].split("')")[0]

        floor_plan_images = response.css('a.sketchBig').attrib['href']

        if response.css('li.noPets'):
            pets_allowed = False

        washing_machine = None
        if response.css('li.Washing.Machine'):
            washing_machine = True

        balcony = None
        if response.css('li.Balcony'):
            balcony = True

        monetary_ameneties = response.css(
            'div.details.detailsAppartment > ul > li')

        property_type = 'apartment'
        rent = None
        deposit = None
        room_count = None
        floor = None
        square_meters = None
        for item in monetary_ameneties:
            if "Monthly Rent:" in item.css('p.car_attr::text').get():
                rent = item.css('p.car_prop::text').get().split("€")[1]
            elif "Security Deposit (3 month rent):" in item.css('p.car_attr::text').get():
                deposit = item.css('p.car_prop::text').get().split("€")[1]
            elif "Bedrooms" in item.css('p.car_attr::text').get():
                room_count = item.css('p.car_prop > a::text').get()
            elif "Size" in item.css('p.car_attr::text').get():
                square_meters = item.css('p.car_prop > a::text').get()
            elif "Floor" in item.css('p.car_attr::text').get():
                floor = item.css('p.car_prop > a::text').get()
        if 'studio' in room_count.lower():
            room_count = 1
            property_type = 'studio'
        coords = response.xpath(
            '//script[contains(.,"L.marker([")]/text()').get()
        coords = extract_coordinates_regex(coords)
        latitude = coords[0]
        longitude = coords[1]

        zipcode, city, address = extract_location_from_coordinates(
            longitude, latitude)

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value(
            "external_source", self.external_source)  # String

        item_loader.add_value("external_id", id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", latitude)  # String
        item_loader.add_value("longitude", longitude)  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        # item_loader.add_value("bathroom_count", bathroom_count) # Int

        # String => date_format
        item_loader.add_value("available_date", available_date)

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        # item_loader.add_value("furnished", furnished) # Boolean
        # item_loader.add_value("parking", parking) # Boolean
        # item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        # item_loader.add_value("terrace", terrace) # Boolean
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        item_loader.add_value("floor_plan_images", floor_plan_images)  # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Belong living')  # String
        # item_loader.add_value("landlord_phone", landlord_number) # String
        # item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
