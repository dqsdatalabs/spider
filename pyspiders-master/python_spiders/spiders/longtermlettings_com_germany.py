# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Longtermlettings_com_germanySpider(Spider):
    name = 'longtermlettings_com_germany'
    name2 = 'longtermlettings_com'
    country='germany'
    locale='de' 
    external_source = "{}_PySpider_{}_{}".format(name2.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.longtermlettings.com"]
    start_urls = ["https://www.longtermlettings.com/find/rentals/germany/"]
    position = 1

    def parse(self, response):
        for url in response.css("div.searchrestable div.prop_title").css("a::attr(href)").getall():
            url = str(url)
            if( not re.search(r'^https?:\/\/.*[\r\n]*', url)):
                continue
            yield Request(response.urljoin(url), callback=self.populate_item)
        
        next_page = response.css("div.search_page").css("a:contains('Next Page')::attr(href)").get()
        if (next_page):
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h2.largetitle::text").get()
        rent = response.css("span.advert-price b::text").get()
        if( not rent ):
            return
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        if(not re.search("([0-9]+)", rent)):
            return
        currency = "EUR"

        external_id = response.css("span.calendars::text").getall()
        external_id = " ".join(external_id)
        external_id = re.findall(r"\(ID: (.+)\)", external_id)[0]
        images_url = f"https://www.longtermlettings.com/r/img_view/img/?id={external_id}&type=R"

        images = requests.get(images_url)
        images = images.text
        images = re.findall(r'<img \s+ src=\"(.+)\"', images)

        property_data = response.css("div:contains('Bedrooms')::text").getall()
        property_data = " ".join(property_data)
        room_count = re.findall("Bedrooms: ([1-9])", property_data)
        if(len(room_count)>0):
            room_count = room_count[0]
        else:
            room_count = "1"

        square_meters = re.findall("Size: ([0-9]+)", property_data)
        if(len(square_meters) > 0):
            square_meters = square_meters[0]
        else:
            square_meters = None
        description = response.css("div.descriptiontext::text").getall()
        description = " ".join(description)
        description = re.sub(r'[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*', "", description)
        description = re.sub(r'^https?:\/\/.*[\r\n]*', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\-[0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'[0-9]+\.[0-9]+\.[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r'\([0-9]\)+ [0-9]+\-[0-9]+', '', description, flags=re.MULTILINE)
        description = re.sub(r"\s+", " ", description)

        address = response.css("span[itemprop='streetAddress']::text").get()
        zipcode = response.css("span[itemprop='postalCode']::text").get()   
        city = response.css("span[itemprop='addressLocality']::text").get()

        address = f"{address}, {city}, {zipcode}"

        location_data = extract_location_from_address(address)
        latitude = str(location_data[1])
        longitude = str(location_data[0])


        landlord_name = "longtermlettings"        
        landlord_phone = "(44) (0)208-150-9171"

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("external_source", self.external_source) 

        item_loader.add_value("external_id", external_id) 
        item_loader.add_value("position", self.position) 
        self.position += 1
        item_loader.add_value("title", title) 
        item_loader.add_value("description", description) 

        item_loader.add_value("city", city) 
        item_loader.add_value("zipcode", zipcode) 
        item_loader.add_value("address", address) 
        item_loader.add_value("latitude", latitude) 
        item_loader.add_value("longitude", longitude) 
        item_loader.add_value("property_type", property_type)  
        item_loader.add_value("square_meters", square_meters) 
        item_loader.add_value("room_count", room_count) 

        item_loader.add_value("images", images) 
        item_loader.add_value("external_images_count", len(images)) 

        item_loader.add_value("rent_string", rent) 
        item_loader.add_value("currency", currency) 

        item_loader.add_value("landlord_name", landlord_name) 
        item_loader.add_value("landlord_phone", landlord_phone) 

        yield item_loader.load_item()
