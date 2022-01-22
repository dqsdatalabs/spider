# -*- coding: utf-8 -*-
# Author: Mahmoud Wessam
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_coordinates


class MrlodgeComSpider(scrapy.Spider):
    name = "mrlodge_com"
    # start_urls = ['https://www.mrlodge.com/apartments-munich']
    start_urls = ['https://www.mrlodge.com/rent/2-room-apartment-munich-perlach-10903']
    allowed_domains = ["mrlodge.com"]
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for appartment in response.xpath("//input[@name='name_url']/@value").extract():
            url = "https://www.mrlodge.com/rent" + appartment
            yield scrapy.Request(url,
                                 callback=self.populate_item,
                                 )

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.css(
            '#mrl-expose-sidebar > div.grid-container.pd-top-15.pd-bottom-15 > div.sidebar-info.grid-x > div.cell.small-6.medium-5::text').get()

        external_id = external_id.split('ID ')[1].strip()

        title = response.css(
            '#mrl-exp-description > div > div > div > div > h1::text').get()
        furnished = None
        if 'furnished' in title.lower():
            furnished = True

        description = response.css('div.description-content > p::text').get()
        if description == None:
            try:
                description = response.css('#mrl-exp-description li ::text').extract()
                description = ' '.join(description)
            except:
                pass

        swimming_pool = None
        if 'swimming' in description.lower():
            swimming_pool = True

        coords = response.xpath(
            '//*[@id="ehmrlexpose"]/script[2]/text()').get()

        latitude = coords.split('{"lat":')[1].split(',')[0]
        longitude = coords.split('"lng":')[1].split(',')[0]

        zipcode, city, address = extract_location_from_coordinates(
            longitude, latitude)

        ameneties = response.css(
            'span.mrl-icon-list__item')

        washing_machine = None
        dishwasher = None
        elevator = None
        parking = None
        terrace = None
        balcony = None
        for item in ameneties:
            if "Washer" in item.css('span.content::text').get():
                if item.css('span.icon.icon-check-thin'):
                    washing_machine = True
            elif "Dishwasher" in item.css('span.content::text').get():
                if item.css('span.icon.icon-check-thin'):
                    dishwasher = True
            elif "Lift" in item.css('span.content::text').get():
                if item.css('span.icon.icon-check-thin'):
                    elevator = True
            elif "Terrace" in item.css('span.content::text').get():
                if item.css('span.icon.icon-check-thin'):
                    terrace = True
            elif "Garage" in item.css('span.content::text').get():
                if item.css('span.icon.icon-check-thin'):
                    parking = True
            elif "Balcony" in item.css('span.content::text').get():
                if item.css('span.icon.icon-check-thin'):
                    balcony = True

        floor = response.css(
            '#mrl-exp-equipment > div > div > div > div > div > div:nth-child(1) > div.mrl-table__column.cell.medium-9::text').get()

        side_bar = response.css(
            'div.mrl-table.mrl-table--small.mrl-table--divider.mn-top-15.mn-bottom-10 > div.mrl-table__row.grid-x.cl-border-secondary')
        side_bar_internal = response.css(
            'div.mrl-table.mrl-table--small.mrl-table--divider.mn-top-15.mn-bottom-10 > h2 > div.mrl-table__row.grid-x.cl-border-secondary')

        rent = None
        square_meters = None
        for itemaya in side_bar:
            if "Rent per month" in itemaya.css('div.mrl-table__column.label::text').get():
                rent = itemaya.css(
                    'div.mrl-table__column.value::text').extract()[0].split('.')[0].split(' ')[-1]
                rent = rent.replace(",", "")
            elif "Size" in itemaya.css('div.mrl-table__column.label::text').get():
                square_meters = itemaya.css(
                    'div.mrl-table__column.value::text').get().strip().split(" ")[1]

        room_count = None
        for itemaya in side_bar_internal:
            if "Apartment type" in itemaya.css('div.mrl-table__column.label::text').get():
                room_count = itemaya.css(
                    'div.mrl-table__column.value::text').get()[0]

        bathroom_count = None
        try:
            list = response.css('.filter ::text').extract()
            bathroom_count = 0
            for bath in list:
                if 'bathroom' in bath.lower():
                    bathroom_count += 1
            if bathroom_count == 0:
                bathroom_count = None
        except:
            pass
        images = response.css(
            'div.img-holder.swiper-lazy::attr(data-background)').extract()

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value(
            "external_source", self.external_source)  # String

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
        # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("property_type", 'apartment')
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", deposit) # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'MR. LODGE GMBH')  # String
        item_loader.add_value("landlord_phone", '+49 89 340 823 0')  # String
        item_loader.add_value("landlord_email", 'info@mrlodge.de')  # String

        self.position += 1
        yield item_loader.load_item()

        next_page = None
        if response.css('li.mrl-list-prev-next.mrl-list-next'):
            next_page = 'https://www.mrlodge.com' + \
                response.css(
                    'li.mrl-list-prev-next.mrl-list-next > a').attrib['href']

        if next_page:
            yield scrapy.Request(next_page,
                                 callback=self.populate_item,
                                 )
