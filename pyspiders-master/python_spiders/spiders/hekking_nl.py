# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'hekking_nl'
    start_urls = ['https://www.hekking.nl/nl/realtime-listings/consumer'] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1
    external_source = "Hekking_PySpider_netherlands_nl"
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        
        for item in data:
            if item["isRentals"] and str(item["rooms"]) != "0": 
                follow_url = response.urljoin(item["url"])

                status = item["status"]
                if status and "Verhuurd" in status:
                    continue

                property_type = item.get('mainType')
                if property_type != ("apartment" or "house"):
                    property_type = None
            
                meta_data = {
                    "lat" : str(item["lat"]),
                    "lng" : str(item["lng"]),
                    "address" : str(item["address"]),
                    "zipcode" : str(item["zipcode"]),
                    "city" : str(item["city"]),
                    "price" : str(item["rentalsPrice"]),
                    "square_meters" : str(item["livingSurface"]),
                    "room_count" : str(item["rooms"]),
                    "furnished" : str(item["isFurnished"]),
                    "property_type" : property_type,
                    "balcony" : item["balcony"] if "balcony" in item else ""

                   
                }
                yield Request(follow_url, callback=self.populate_item, meta = meta_data)


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)

        if response.xpath("//h3[contains(.,'Woning niet meer beschikbaar')]").get(): return
        
        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        address = response.meta.get("address")
        zipcode = response.meta.get("zipcode")
        city = response.meta.get("city")
        price = response.meta.get("price")
        square_meters = response.meta.get("square_meters")
        room_count = response.meta.get("room_count")
        furnished = response.meta.get("furnished")
        balcony = response.meta.get("balcony")
        if furnished == "true":
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        property_type = response.meta.get('property_type')
        if property_type:
            item_loader.add_value("property_type", property_type)
        else:
            return

        if balcony != "":
            item_loader.add_value("balcony", balcony)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("city", city)
        item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)  
        item_loader.add_value("zipcode", zipcode)
        if city:
            address = address+", "+city
        item_loader.add_value("address", address)

        description = "".join(response.xpath("//p[@class='object-description']//text()").getall())
        if description:
            item_loader.add_value("description", description.split())
        else:
            description = "".join(response.xpath("//div[@class='expand-content-content']/text()").getall())
            if description:
                item_loader.add_value("description", description.split())

        utilities = response.xpath("//dl[@class='full-details']/dt[.='Servicekosten']/following-sibling::dd[1]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace("€","").strip())  

        images = [x for x in response.xpath("//div[contains(@class,'swiper-slide')]/img[1]/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor = response.xpath("//dt[contains(.,'verdiepingen')]/following-sibling::dd[1]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        energy_label = response.xpath("//dl[@class='full-details']/dt[.='Energielabel']/following-sibling::dd[1]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        elevator = response.xpath("//dl[@class='full-details']/dt[.='Voorzieningen']/following-sibling::dd[1]/text()").get()
        if elevator:
            if "Lift" in elevator:
                item_loader.add_value("elevator", True)
             
        parking = response.xpath("//dl[@class='full-details']/dt[.='Soort garage']/following-sibling::dd[1]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        bathroom_count = response.xpath("//dt[contains(.,'badkamers')]/following-sibling::dd[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("landlord_name", "Hekking Makelaardij")
        item_loader.add_value("landlord_phone", "070 404 98 98")
        item_loader.add_value("landlord_email", "info@hekking.nl ")
        yield item_loader.load_item()