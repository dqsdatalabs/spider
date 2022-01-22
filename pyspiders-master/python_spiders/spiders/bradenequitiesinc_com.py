# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class BradenequitiesincSpider(Spider):
    name = 'bradenequitiesinc_com'
    country='Canada'
    locale='en' 
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    execution_type='testing'
    allowed_domains = ["www.bradenequitiesinc.com"]
    start_urls = ["https://www.bradenequitiesinc.com/property-listings/?price=&style=&search=true&city="]

    def parse(self, response):
        for url in response.css("div.card-item-wrapper a.property-card::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item )

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = "apartment"
        rent = response.css("div.property-price::text").get()
        currency = "CAD"
        title = response.css("h1.white-heading::text").get()

        square_meters = response.css("li.full::text").get()
        room_count = response.css("ul.property-icons:nth-child(1) > li:nth-child(1)::text").get()
        bathroom_count = response.css("ul.property-icons:nth-child(1) > li:nth-child(2)::text").get()
        
        description = response.css("div.property-description p::text").getall()
        description = " ".join(description)

        deposit = response.css("div.property-deposit::text").get().split(" ")[1]

        balcony = None
        parking = None
        balcony = response.css("li:contains('Balcony')::text").get()
        parking = response.css("li:contains('Parking:')::text").get()
        if parking:
            parking = True
        if balcony: 
            balcony = True

        images_to_add = []        
        images = response.css("img.lightgallery-thumb::attr(src)").getall()
        for image_src in images:
            image_src = re.sub("/thumbs", "", image_src)
            images_to_add.append(response.urljoin(image_src))

        pets_allowed = None
        pets_allowed = response.css("span.info-label:contains('Pets:') + span.info-value::text").get()
        if pets_allowed:
            pets_allowed = True

        city = None
        zipcode = None
        address = response.css("div#property-map::attr(data-address)").get()
        try:
            city = address.split(",")[1]
            zipcode = address.split(",")[0]
        except:
            pass

        latitude = response.css("div#property-map::attr(data-gpslat)").get()
        longitude = response.css("div#property-map::attr(data-gpslong)").get()
        
        landlord_email = "info@bradenequitiesinc.com"
        landlord_name = "bradenequitiesinc"
        landlord_phone = "(780) 429-5956"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", int(int(square_meters)*10.764))
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("description", description)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("parking", parking)
        item_loader.add_value("images", images_to_add)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()
