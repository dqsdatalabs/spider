# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'tvnrealestate_nl'
    execution_type='testing'
    country='netherlands'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.tvnrealestate.nl/en/realtime-listings/consumer"}, 
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                        )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:
            if item["isRentals"] and not item["isSales"] and ("Available" in item["status"]):
                item_loader = ListingLoader(response=response)
                follow_url = response.urljoin(item["url"])
                item_loader.add_value("latitude", str(item["lat"]))
                item_loader.add_value("longitude", str(item["lng"]))
                item_loader.add_value("address", item["address"])
                item_loader.add_value("zipcode", item["zipcode"])
                item_loader.add_value("city", item["city"])
                item_loader.add_value("rent", item["rentalsPrice"])
                item_loader.add_value("currency", "EUR")
                room_count = item["bedrooms"]
                if room_count != 0:
                    item_loader.add_value("room_count", room_count)
                else:
                    item_loader.add_value("room_count", item["bedrooms"])
                
                meters = item["livingSurface"]
                if meters != 0:
                    item_loader.add_value("square_meters", meters)

                furnished = item["isFurnished"]
                if furnished == False or furnished =="No":
                    item_loader.add_value("furnished", False)
                elif furnished == True or furnished == "Yes":
                    item_loader.add_value("furnished", True)

                balcony = item["balcony"]
                if balcony == False or balcony == "No":
                    item_loader.add_value("balcony", False)
                elif balcony == True or balcony =="Yes" :
                    item_loader.add_value("balcony", True)
                
                property_type = ""
                if "apartment" in item["mainType"]:
                    property_type = "apartment"
                elif "house" in item["mainType"]:
                    property_type ="house"
                else:
                    property_type = False
                if property_type:
                    yield Request(follow_url, callback=self.populate_item, meta={"item":item_loader, "property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = response.meta.get("item")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)
        item_loader.add_value("external_source", "Tvnrealestate_PySpider_netherlands")

        property_type = response.xpath("//dd[contains(.,'Studio') or contains(.,'studio')]").get()
        if property_type: item_loader.add_value("property_type", "studio")
        else: item_loader.add_value("property_type", response.meta["property_type"])

        bathroom = "".join(response.xpath("//dl[@class='full-details']/dt[contains(.,'Bathrooms')]/following-sibling::dd[1]//text()").extract())
        if bathroom:
            item_loader.add_value("bathroom_count", int(float(bathroom)))
    
        floor = "".join(response.xpath("//dl[@class='full-details']/dt[contains(.,'Number of floors')]/following-sibling::dd[1]//text()").extract())
        if floor:
            item_loader.add_value("floor", floor.strip())

        desc = "".join(response.xpath("//div[@class='expand-content']//text()").extract())
        if desc:
            item_loader.add_value("description", re.sub(r'\s{2,}', ' ', desc.strip()))

        images = [x for x in response.xpath("//div[@class='col-xs-12']/div[contains(@class,'gallery ')]/div//@data-src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "+31(0)20-3791187")
        item_loader.add_value("landlord_name", "TVN Real Estate")
        item_loader.add_value("landlord_email", "info@tvnrealestate.nl")
        

        yield item_loader.load_item()