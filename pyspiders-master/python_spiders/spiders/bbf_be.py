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
    name = 'bbf_be'
    execution_type = 'testing' 
    country='belgium'
    locale='en'
    external_source = 'Bbf_PySpider_belgium'
    post_url = "https://www.bbf.be/wp-admin/admin-ajax.php"  
    
    headers = {    
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    }
    
    def start_requests(self):
        yield Request(
            url="https://www.bbf.be/search-results/?area=&listing_type=&guest=&room_size=&min-price=&max-price=",
            callback=self.jump,
        )
    
    def jump(self, response):  
        security_data = response.xpath("//input[@id='securityhomeyMap']/@value").get()
          
        formdata = {
            "action": "homey_half_map",
            "arrive": "",
            "depart": "",
            "guest": "",
            "keyword": "",
            "pets": "",
            "bedrooms": "",
            "rooms": "",
            "room_size": "",
            "search_country": "",
            "search_city": "",
            "search_area": "",
            "min-price": "",
            "max-price": "",
            "country": "",
            "state": "",
            "city": "",
            "area": "",
            "booking_type": "",
            "search_lat": "",
            "search_lng": "",
            "radius": "",
            "start_hour": "",
            "end_hour": "",
            "amenity": "",
            "facility": "",
            "layout": "card",
            "num_posts": "100",
            "sort_by": "a_price",
            "paged": "0",
            "security": security_data,
        }
        
        yield FormRequest(
            url=self.post_url,
            dont_filter=True,
            formdata=formdata,
            headers=self.headers,
            callback=self.parse
        )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)["listings"]
        for item in data:
            follow_url = item["url"]
            yield Request(follow_url, callback=self.populate_item, meta={"item":item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
       
        item = response.meta.get("item")
        
        studio_check = response.xpath("//div[contains(.,'Type')]/following-sibling::div/strong/text()").get()
        if studio_check and "studio" in studio_check.lower():
            property_type = "studio"
        else:
            property_type = "apartment"
        item_loader.add_value("property_type", property_type)
        
        external_id = item["id"]
        if external_id:
            item_loader.add_value("external_id", str(external_id))

        title = response.xpath("//h1[@class='listing-title']/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        rent = item["price"]
        if rent and "month" in rent.lower():
            rent = rent.split("€</sup>")[1].split("<")[0]
            item_loader.add_value("rent", rent.replace(",",""))
        elif rent and "night" in rent.lower():
            rent = rent.split("€</sup>")[1].split("<")[0].replace(",","")
            rent = int(rent)*30
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        room_count = item["beds"]
        if room_count:
            item_loader.add_value("room_count", room_count)
        bathroom_count = item["baths"]
        if bathroom_count:
            bathroom_count = str(bathroom_count).split(".")[0]
            item_loader.add_value("bathroom_count", int(bathroom_count))
        
        address = item["address"]
        if address:
            item_loader.add_value("address", address)

        deposit = response.xpath("//li[contains(.,'deposit')]/strong/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("€", "").replace(",", "").strip())

        lat = item["lat"]
        if lat:
            item_loader.add_value("latitude", lat)
        lng = item["long"]
        if lng:
            item_loader.add_value("longitude", lng)
        
        elevator = response.xpath("//li/text()[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        dishwasher = response.xpath("//li/text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        parking = response.xpath("//li/text()[contains(.,'arking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        desc = "".join(response.xpath("//div[@class='block-body']/p/text()").getall())
        if desc:
            description = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//a[@data-fancybox='gallery']/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "BBF Apartments")
        item_loader.add_value("landlord_phone", "+32 (0)2 705 05 21")
        item_loader.add_value("landlord_email", "info@bbf.be")
        
        yield item_loader.load_item()
