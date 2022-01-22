# -*- coding: utf-8 -*-
# Author: Marwan Eid
import datetime
from typing import Type
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
from ..helper import Amenties, remove_white_spaces, extract_location_from_address, extract_location_from_coordinates, get_amenities, format_date, extract_number_only, convert_string_to_numeric

def custom_date_format(date):
    return date.replace(" ", ".").replace("..", ".").replace("Januar", "01").replace("Februar", "02").replace("März", "03").replace("April", "04").replace("Mai", "05").replace("Juni", "06").replace("Juli", "07").replace("August", "08").replace("September", "09").replace("Oktober", "10").replace("November", "11").replace("Dezember", "12")


class KönigshofimmobilienPyspiderGermanyDeSpider(scrapy.Spider):
    name = "konigshofimmobilien"
    start_urls = [
            {
                "url": ["https://www.koenigshof-immobilien.de/immobilien-vermarktungsart/miete/?typ=wohnung"],
                "property_type": "apartment"
            }
    ]
    allowed_domains = ["koenigshof-immobilien.de"]
    country = 'germany'
    locale = 'de'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            for item in url.get('url'):
                yield Request(url=item, callback=self.parse, meta={'property_type': url.get('property_type')})

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        for item in response.xpath("//div[contains(@class,'property-thumbnail col-sm-5')]//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})


    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        is_rented = response.xpath("//*[contains(text(), 'vermietet')]").getall()
        if len(is_rented) > 0:
            return

        # # MetaData
        external_id = response.xpath("//div[@class='dd col-sm-7']//text()").extract()[0]
        title = response.xpath("//h1[@class='property-title']//text()").extract()[0]
        description_paragraphs = response.xpath("//div[@class='panel-body']/p//text()").getall()
        description_paragraphs = description_paragraphs[: -2]
        description = ""
        Amenties_text = ""
        for para in description_paragraphs:
            if "Ausstattung" in para:
                break
            description += para
        description = remove_white_spaces(description)
        amenities_paragraphs = response.xpath("//*[contains(text(), 'Ausstattung')]/following-sibling::p[1]/text()").getall()
        for para in amenities_paragraphs:
            if "Ausstattung" in para:
                break
            Amenties_text += para
        Amenties_text = remove_white_spaces(Amenties_text)
        address_paragraphs = response.xpath("//*[contains(text(), 'Adresse')]/following-sibling::div[1]/text()").getall()
        address = ""
        for para in address_paragraphs:
            if "eideckstr" in para:
                break
            address += para
        address = remove_white_spaces(address)
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, _ = extract_location_from_coordinates(longitude, latitude)
        floor_list = response.xpath("//*[contains(text(), 'Etage')]/following-sibling::div[1]/text()").getall()
        floor = floor_list[0] if len(floor_list) > 0 else ""
        square_meters = response.xpath("//*[contains(text(), 'Wohnfläche')]/following-sibling::div[1]/text()").getall()[0]
        square_meters = int(square_meters[: square_meters.find(" ")])
        room_count = response.xpath("//*[contains(text(), 'Zimmer­anzahl')]/following-sibling::div[1]/text()").getall()[0]
        room_count = int(convert_string_to_numeric(room_count, KönigshofimmobilienPyspiderGermanyDeSpider))
        bathroom_count = response.xpath("//*[contains(text(), 'Badezimmer')]/following-sibling::div[1]/text()").getall()[0]
        available_date = remove_white_spaces(response.xpath("//*[contains(text(), 'Verfügbar ab')]/following-sibling::div[1]/text()").getall()[0])
        get_amenities(description, Amenties_text, item_loader)
        available_date = custom_date_format(available_date)
        available_date = format_date(available_date, date_format="%d.%m.%Y")
        if "-" not in available_date:
            available_date = None
        images = response.xpath("//div[@id='immomakler-galleria']//@src").extract()
        for index, url in enumerate(images):
            if index > 0:
                images[index] = url[: -10] + ".jpg"
        rent = response.xpath("//*[contains(text(), 'Kaltmiete')]/following-sibling::div[1]/text()").getall()[0]
        deposit = response.xpath("//*[contains(text(), 'Kaution')]/following-sibling::div[1]/text()").getall()
        deposit = deposit[0] if len(deposit) > 0 else 0
        extra1 = response.xpath("//*[contains(text(), 'Nebenkosten')]/following-sibling::div[1]/text()").getall()[0]
        extra2 = response.xpath("//*[contains(text(), 'Betriebskosten (netto)')]/following-sibling::div[1]/text()").getall()[0]
        extra3 = response.xpath("//*[contains(text(), 'Warmmiete')]/following-sibling::div[1]/text()").getall()
        extra3 = extra3[0] if len(extra3) > 0 else rent
        rent = int(extract_number_only(rent))
        deposit = int(extract_number_only(deposit))
        utilities = int(extract_number_only(extra1)) + int(extract_number_only(extra2))
        heating_cost = int(extract_number_only(extra3)) - rent
        landlord_name = remove_white_spaces(response.xpath("//*[contains(text(), 'Name')]/following-sibling::div[1]/text()").getall()[0])
        landlord_number = response.xpath("//*[contains(text(), 'Tel.')]/following-sibling::div[1]/text()").getall()[0]
        landlord_email = response.xpath("//*[contains(text(), 'E-Mail')]/following-sibling::div[1]/text()").getall()[0]

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
        item_loader.add_value("property_type", response.meta.get('property_type'))
        #item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        #item_loader.add_value("elevator", elevator) # Boolean
        #item_loader.add_value("balcony", balcony) # Boolean
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
        item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        #item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
