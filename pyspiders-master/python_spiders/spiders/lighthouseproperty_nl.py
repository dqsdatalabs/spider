# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'lighthouseproperty_nl'   
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' 
    external_source = 'Lighthouseproperty_PySpider_netherlands'
    start_urls = ['https://www.lighthouseproperty.nl/nl/realtime-listings/consumer'] 
    # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        for item in data:
            if item["isRentals"] == False:
                continue
            if item["status"] == "Verhuurd":
                continue
            follow_url = response.urljoin(item["url"])
            lat = item["lat"]
            lng = item["lng"]
            property_type = item["mainType"]
            address = item["address"]
            zipcode = item["zipcode"]
            city = item["city"]
            room = item["rooms"]
            squ = item["livingSurface"]
            
            if "apartment" in property_type:
                yield Request(follow_url,callback=self.populate_item, meta={'lat':lat, 'lng':lng, 'address':address, 'zipcode': zipcode, 'city':city, 'room': room, 'squ': squ, 'property_type': "apartment"})
            elif "house" in property_type:
                yield Request(follow_url,callback=self.populate_item, meta={'lat':lat, 'lng':lng, 'address':address, 'zipcode': zipcode, 'city':city, 'room': room, 'squ': squ, 'property_type': "house"})
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
     
        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        address = response.meta.get("address")
        zipcode = response.meta.get("zipcode")
        city = response.meta.get("city")     
        room_count = response.meta.get("room")
        square_meters = response.meta.get("squ")

        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)

        title = response.xpath("normalize-space((//h1/text())[1])").extract_first()
        item_loader.add_value("title", title)
        
        price=response.xpath("//dl[@class='details-simple']/dt[.='Prijs']/following-sibling::dd[1]/text()[1]").extract_first()
        if price:
            item_loader.add_value("rent",price.split("€")[1].split("p")[0])
        item_loader.add_value("currency","EUR")
        
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        if city:
            item_loader.add_value("city", city)
        
        if address:
            item_loader.add_value("address", address)
        
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

        if lat:
            item_loader.add_value("latitude", str(lat))
        
        if lng:
            item_loader.add_value("longitude", str(lng))

        bathroom_count = response.xpath("//dl[@class='details-simple']/dt[.='Badkamers']/following-sibling::dd[1]/text()").get()
        if bathroom_count:
            item_loader.add_xpath("bathroom_count", bathroom_count)
        
        energy_label = response.xpath("//dl[@class='details-simple']/dt[contains(.,'Energielabel')]/following-sibling::dd[1]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        balcony = response.xpath("//dl[@class='details-simple']/dt[contains(.,'Balkon')]/following-sibling::dd[1]/text()[contains(.,'Ja')]").get()
        if balcony:
            item_loader.add_value("balcony", True) 

        parking = response.xpath("//dl[@class='details-simple']/dt[contains(.,'Parkeren')]/following-sibling::dd[1]/text()").get()
        if parking and "garage" in parking.lower():
            item_loader.add_value("parking", True)
        elif parking and "parkeerplaat" in parking.lower():
            item_loader.add_value("parking", True)

        available_date = response.xpath("//dl[@class='details-simple']/dt[contains(.,'Oplevering')]/following-sibling::dd[1]/text()").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        desc="".join(response.xpath("//p[@class='object-description']/text()").extract())
        if desc:        
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description",desc)    

        images = [response.urljoin(x) for x in response.xpath("//div[@class='responsive-slider-slide']/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)   

        item_loader.add_value("landlord_name", "Lighthouse Property Services B.V.")
        item_loader.add_value("landlord_phone", "+31 23 30 30 399")
        item_loader.add_value("landlord_email", "info@lighthouseproperty.nl")
        
        yield item_loader.load_item()