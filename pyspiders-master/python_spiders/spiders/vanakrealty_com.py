# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import requests

from python_spiders.helper import extract_location_from_coordinates, extract_location_from_address
from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class VanakrealtySpider(Spider):
    name = 'vanakrealty_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.vanakrealty.com"]
    start_urls = ["https://vanakrealty.com/rentals/mls/?limit=100&wplpage=1"]

    def parse(self, response):
        for url in response.css("a.noHover::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)
        
        next_page = response.css("ul.pagination li.next a::attr(href)").get()
        if next_page != "#":
            yield response.follow(response.urljoin(next_page), callback=self.parse, dont_filter = True)

    def populate_item(self, response):
        property_state = response.css("h1.title_text::text").get().strip().lower()
        if(property_state == "rented"):
            return
        if(property_state == "office"):
            return
        
        property_type = "apartment"
        title = property_state

        floor = response.css("label:contains('Floor Number :') + span::text").get()
        external_id = response.css("#wpl-dbst-show5 > span:nth-child(1)::text").get()
  
        rent = response.css("#wpl-dbst-show6 > span:nth-child(1)::text").get()
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        if(rent == "0"):
            return
        
        currency = "CAD"

        room_count = response.css("#wpl-dbst-show8 > span:nth-child(1)::text").get()
        if(room_count == None):
            room_count = "1"
        room_count = room_count.strip()
        if(room_count == ""):
            room_count = "1"

        bathroom_count = response.css("#wpl-dbst-show9 > span:nth-child(1)::text").get()
        square_meters = response.css("span:contains('Sqft')::text").get()
        try:
            square_meters = re.findall("([0-9]+)", square_meters)
            square_meters = "".join(square_meters)
        except: 
            pass

        description = response.css("div.wpl_prp_show_detail_boxes_cont p::text").getall()
        description = " ".join(description)

        images = response.css("ul.wpl-gallery-pshow li span img::attr(src)").getall()
        if(len(images) < 3):
            return
        
        concatenated_images = " ".join(images)
        rented_image_exist = "img_2.jpg" in concatenated_images
        if(rented_image_exist):
            return

        landlord_name = "Moe Moghadasian"
        landlord_phone = "604.219.9744"
        landlord_email = "moe@vanakproperties.com"
        furnished = None
        if(description):
            description = re.sub(r"[0-9]+\-[0-9]+\-[0-9]", "", description, flags=re.MULTILINE)
            description = re.sub(r"[0-9]+\.[0-9]+\.[0-9]", "", description, flags=re.MULTILINE)
            description = re.sub(r"[A-Za-z0-9]*@[A-Za-z]*\.?[A-Za-z0-9]*", "", description, flags=re.MULTILINE)
            description = re.sub(r"^https?:\/\/.*[\r\n]*", "", description, flags=re.MULTILINE)
            furnished = "furnished" in description
        
        if( not furnished):
            furnished = response.css("span:contains('Furnished')::text").get()
            if(furnished):
                furnished = True
            else: 
                furnished = False

        script_location = response.css("script:contains('ws_lat')::text").get()
        latitude = re.findall("var ws_lat = '(-?[0-9]+\.[0-9]+)';", script_location)
        longitude = re.findall("var ws_lon = '(-?[0-9]+\.[0-9]+)';", script_location)
        address = re.findall("var ws_address = '(.+)';", script_location)

        if(len(address) > 0):
            address = address[0]
        
        if( len(latitude) > 0 and not re.search("(-?0+\.0+)", latitude[0]) ):
            latitude = str(latitude[0])
        else: 
            latitude = None

        if(len(longitude) > 0 and not re.search("(-?0+\.0+)", longitude[0])):
            longitude = str(longitude[0])
        else: 
            longitude = None

        if(latitude and longitude):
            try:

                location_data = extract_location_from_coordinates(longitude, latitude)
                zipcode = location_data[0]
                city = location_data[1]
            except:
                zipcode = None
                city = None
        else: 
            location_data = extract_location_from_address(address)
            longitude = str(location_data[0])
            latitude = str(location_data[1])

        try:
            location_data = extract_location_from_coordinates(longitude, latitude)
            zipcode = location_data[0]
            city = location_data[1]
        except:
            zipcode = None
            city = None

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("floor", floor)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("description", description)
        item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("furnished", furnished)
       
        yield item_loader.load_item()
