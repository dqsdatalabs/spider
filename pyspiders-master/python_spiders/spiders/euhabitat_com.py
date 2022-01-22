# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_coordinates, extract_location_from_address

class AntonellianaSpider(Spider):
    name = 'euhabitat_com'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.euhabitat.com"]
    start_urls = ["https://www.euhabitat.com/"]
    position = 1


    def parse(self, response):
        for url in response.css("div.vc_btn3-container a.vc_general::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.get_properties_page)
        
  
    def get_properties_page(self, response):
        for url in response.css("div.listing_wrapper div.property_listing h4 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)

        next_page = response.css("ul.pagination li.roundright a::attr(href)").get()
        if(next_page != response.url):
            yield response.follow(response.urljoin(next_page), callback=self.get_properties_page)

    def populate_item(self, response):
        
        property_type = "apartment"
        title = response.css("h1.entry-title::text").get()
        rent = response.css("span.price_area::text").get()
        if(not re.search("([0-9]+)", rent)):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"

        images = response.css("div.item img::attr(src)").getall()

        zipcode = response.css("div.listing_detail:contains('Zip:')::text").get()
        city = response.css("div.listing_detail:contains('City:')::text").get()
        area = response.css("div.listing_detail:contains('Area:')::text").get()

        address = f"{city}, {zipcode}"
        if(area):
            address = f"{address}, {area}"
        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])

        location_data = extract_location_from_coordinates(longitude, latitude)
        address = location_data[2]
        city = location_data[1]
        zipcode = location_data[0]

        external_id = response.css("div.listing_detail:contains('Object-ID:')::text").get()
        square_meters = response.css("div.listing_detail:contains('Property Size:')::text").get()
        room_count = response.css("div.listing_detail:contains('Rooms:')::text").get()
        energy_label = response.css("div.listing_detail:contains('Energy class:')::text").get()
        floor = response.css("div.listing_detail:contains('Floor:')::text").get()
        available_date = response.css("div.listing_detail:contains('Available from:')::text").get()
        deposit = response.css("div.listing_detail:contains('Deposit:')::text").get()
        description = response.css("div.wpestate_property_description p::text").getall()
        description = " ".join(description)

        amenities = response.css("div.panel-body div.listing_detail::text").getall()
        amenities = " ".join(amenities)

        furnished = "furnished" in description
        washing_machine = "washing machine" in description or "laundry" in description
        dishwasher = "dishwasher" in description
        balcony = "balcon" in description
        parking = "garage" in description
        landlord_name = "euhabitat"

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("position", self.position)
        self.position += 1
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("images", images)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("floor", floor)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("description", description)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("parking", parking)
        item_loader.add_value("landlord_name", landlord_name)

        yield item_loader.load_item()
