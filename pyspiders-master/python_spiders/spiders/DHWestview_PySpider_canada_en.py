# -*- coding: utf-8 -*-
# Author: Muhammad Alaa
import builtins
import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import sq_feet_to_meters, extract_location_from_address, extract_location_from_coordinates
import math



class DhwestviewPyspiderCanadaEnSpider(scrapy.Spider):
    name = "DHWestview_PySpider_canada_en"
    start_urls = [  'https://www.dhwestview.com/cities/etobicoke',
                    'https://www.dhwestview.com/cities/oakville',
                    'https://www.dhwestview.com/townhomes'
                 ]
    allowed_domains = ["dhwestview.com"]
    country = 'canada' 
    locale = 'en'
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
        buildings = response.css("section.property-list a.property-item")
        for building in buildings:
            url = "https://www.dhwestview.com" + building.css("a::attr(href)").get()
            yield scrapy.Request(url, callback=self.populate_item)


    # 3. SCRAPING level 3
    def populate_item(self, response):
        property_type = "apartment"
        description = response.css("div.cms-content p::text").get()
        address = response.css("div.mainbar.span6 h4.property-address::text").get().strip()
        title = response.css("div.mainbar.span6 h1.property-title::text").get().strip()
        utilities = response.css("section.widget.utilities span::text").getall()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
        landlord_phone = response.css("section.contact-area h2::text").get()
        amenities = response.css("section.widget.amenities ul li::text").getall()
        images = []
        elevator = False
        if "Elevators" in amenities:
            elevator = True
        parking = False
        if "Parks nearby" in amenities or "Underground parking" in amenities or "Visitor parking" in amenities:
            parking = True
            
        washing_machine = False
        if "Laundry facilities" in amenities:
            washing_machine = True
            

        for inx, img in enumerate(response.xpath("//*[contains(@id, 'gallery')]/div/ul[1]/li")):          
            images.append(img.xpath("//*[contains(@id, 'gallery')]/div/ul[1]/li[" + str(inx + 1) +"]/div[2]/img").xpath("@src").get())  
        for inx, appartment in enumerate(response.css("table.suites.has-floorplans tbody tr")):
            item_loader = ListingLoader(response=response)
            rooms = appartment.css("td.suite-type-name::text").get()
            if rooms.find("Bachelor") != -1:
                room_count = 1
            else:
                room_count = rooms[0]

            bathroom_count = math.floor(int(appartment.css("td.suite-bath span.value::text").get()))
            square_meters = appartment.css("td.suite-sq-ft span.value::text").get()
            landlord_name = "DH Westview"
            if square_meters:
                square_meters = sq_feet_to_meters(square_meters)
                item_loader.add_value("square_meters", square_meters)

            rent = appartment.css("td.suite-rate::text").getall()[1]
            floor_plan_images = appartment.css("td.floorplan-td.requires-js a::attr(href)").getall()
            item_loader.add_value("external_link", response.url+"#" + str(inx + 1))
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("position", self.position)
            item_loader.add_value("title", title) 
            item_loader.add_value("description", description) 
            item_loader.add_value("city", city)
            item_loader.add_value("address", address) 
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("room_count", room_count) 
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            item_loader.add_value("floor_plan_images", floor_plan_images)
            item_loader.add_value("rent", rent)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('latitude', str(latitude))
            item_loader.add_value('longitude', str(longitude))
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value('landlord_phone', landlord_phone)
            item_loader.add_value("currency", "CAD")
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("parking", parking)
            item_loader.add_value("washing_machine", washing_machine)
            
            balcony = False
            if "Balconies" in amenities and rooms.find("No Balny") != -1:
                balcony = True
            item_loader.add_value("balcony", balcony)
            for util in utilities:
                if util == "Water":
                    water_cost = 0
                    item_loader.add_value("water_cost", water_cost)
                if util == "Heat":
                    heater_cost = 0
                    item_loader.add_value("heating_cost", heater_cost)
            self.position += 1
            yield item_loader.load_item()
