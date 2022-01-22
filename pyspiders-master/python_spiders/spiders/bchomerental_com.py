# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class BchomerentalSpider(Spider):
    name = 'bchomerental_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.bchomerental.com"]
    start_urls = ["https://www.bchomerental.com/property-status/available/"]

    def parse(self, response):
        for url in response.css("div.rh_list_card__details h3 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        title = response.css("h1.rh_page__title::text").get()
        rent = response.css("p.price::text").get()
        rent = re.findall("[0-9]+", rent)
        rent = "".join(rent)
        currency = "CAD"

        address = response.css("p.rh_page__property_address::text").get()
        city = response.css(".property-breadcrumbs > ul:nth-child(1) > li:nth-child(2) > a:nth-child(1)::text").get()
        zipcode = address.split(",")[-2]

        images = response.css("a.swipebox::attr(href)").getall()
        
        external_id = response.css("p.id::text").get()
        
        room_count = response.css("h4:contains('Bedrooms') + div span::text").get()
        bathroom_count = response.css("h4:contains('Bathrooms') + div span::text").get()
        
        parking = response.css("h4:contains('Garage') + div span::text").get()
        if(parking):
            parking = True
        else:
            parking = False

        square_meters = response.css("h4:contains('Area') + div span::text").get()
        square_meters = int(int(float(square_meters))/10.763)
        description = response.css("div.rh_content p::text").getall()
        description = " ".join(description)
        location_script = response.css("script#property-google-map-js-extra::text").get()
        latitude = re.findall("\"lat\":\"(-?[0-9]+\.[0-9]+)\"", location_script)[0]
        longitude = re.findall("\"lng\":\"(-?[0-9]+\.[0-9]+)\"", location_script)[0]

        landlord_name = "bchomerental"
        landlord_phone = "604-337-5333"
        landlord_email = "leasing@bchomerental.com"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("images", images)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("parking", parking)
        item_loader.add_value("square_meters", int(int(square_meters)*10.764))
        item_loader.add_value("description", description)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()
