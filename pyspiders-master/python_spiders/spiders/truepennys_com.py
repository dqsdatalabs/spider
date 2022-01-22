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
from datetime import datetime

class MySpider(Spider):
    name = 'truepennys_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.truepennys.com/?id=2720&action=view&route=search&view=list&input=SW1V&jengo_radius=20&jengo_property_for=2&jengo_property_type=8&jengo_category=1,13,14,6,10,9,8,12,11,3,4,2&jengo_min_price=0&jengo_min_beds=0&jengo_max_price=99999999999&jengo_max_beds=9999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=",
                    "https://www.truepennys.com/?id=2720&action=view&route=search&view=list&input=SW1V&jengo_radius=20&jengo_property_for=2&jengo_property_type=13&jengo_category=1,13,14,6,10,9,8,12,11,3,4,2&jengo_min_price=0&jengo_min_beds=0&jengo_max_price=99999999999&jengo_max_beds=9999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude="
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.truepennys.com/?id=2720&action=view&route=search&view=list&input=SW1V&jengo_radius=20&jengo_property_for=2&jengo_property_type=6&jengo_category=1,13,14,6,10,9,8,12,11,3,4,2&jengo_min_price=0&jengo_min_beds=0&jengo_max_price=99999999999&jengo_max_beds=9999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=",
                    "https://www.truepennys.com/?id=2720&action=view&route=search&view=list&input=SW1V&jengo_radius=20&jengo_property_for=2&jengo_property_type=14&jengo_category=1,13,14,6,10,9,8,12,11,3,4,2&jengo_min_price=0&jengo_min_beds=0&jengo_max_price=99999999999&jengo_max_beds=9999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=",
                    "https://www.truepennys.com/?id=2720&action=view&route=search&view=list&input=SW1V&jengo_radius=20&jengo_property_for=2&jengo_property_type=17&jengo_category=1,13,14,6,10,9,8,12,11,3,4,2&jengo_min_price=0&jengo_min_beds=0&jengo_max_price=99999999999&jengo_max_beds=9999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=",
                    "https://www.truepennys.com/?id=2720&action=view&route=search&view=list&input=SW1V&jengo_radius=20&jengo_property_for=2&jengo_property_type=10&jengo_category=1,13,14,6,10,9,8,12,11,3,4,2&jengo_min_price=0&jengo_min_beds=0&jengo_max_price=99999999999&jengo_max_beds=9999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.truepennys.com/?id=2720&action=view&route=search&view=list&input=SW1V&jengo_radius=20&jengo_property_for=2&jengo_property_type=18&jengo_category=1,13,14,6,10,9,8,12,11,3,4,2&jengo_min_price=0&jengo_min_beds=0&jengo_max_price=99999999999&jengo_max_beds=9999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=",
                    
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(""),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='txt-section-left']"):
            items = {}
            follow_url = response.urljoin(item.xpath(".//a[.='View Details']/@href").get())
            items["room_count"] = item.xpath("//li//i[contains(@class,'bed')]/parent::a/text()").get()
            items["bathroom_count"] = item.xpath("//li//i[contains(@class,'bath')]/parent::a/text()").get()
            items["square_meters"] = item.xpath("//li//i[contains(@class,'square')]/parent::a/text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"], "items": items})
        
        next_page = response.xpath("//a[@class='next-prev']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )  
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Truepennys_PySpider_united_kingdom")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        items = response.meta.get('items')
        
        room_count = response.xpath("//title//text()").get()
        if room_count:
            if "studio" in room_count.lower():
                item_loader.add_value("room_count", "1")
            elif "bed" in room_count.lower():
                room_count = room_count.lower().split("bed")[0].strip()
                item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count",items["bathroom_count"])
        
        address = response.xpath("//h1[contains(@class,'address1')]/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
        
        rent = "".join(response.xpath("//h2//text()").getall())
        if rent:
            price = rent.split(".")[0].replace(",","")
            item_loader.add_value("rent_string", price)
        
        desc = " ".join(response.xpath("//h5[contains(.,'Desc')]/..//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "sq ft" in desc:
            square_meters = desc.split("sq ft")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters)
        
        images = [x for x in response.xpath("//div[@id='details-photo']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        not_list = ["large", "wood", "original", "Boast"]
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            status = True
            for i in not_list:
                if i in floor:
                    status = False
            if status:
                item_loader.add_value("floor", floor)
        
        latitude_longitude = response.xpath("//script[contains(.,'prop_lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('prop_lat =')[1].split(';')[0].strip()
            longitude = latitude_longitude.split('prop_lng =')[1].split(';')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        if "available now" in desc.lower():
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        deposit = response.xpath("//h5[contains(.,'Desc')]/..//p//text()[contains(.,'Security')]").get()
        if deposit:
            price = rent.split(".")[0].replace(",","").split("£")[1]
            deposit = deposit.split(":")[1].strip().split(" ")[0]
            item_loader.add_value("deposit", int(float(int(deposit)*(int(price)/4))))
        
        furnished = response.xpath("//div[@id='features']//text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//div[@id='features']//text()[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//div[@id='features']//text()[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//div[@id='features']//text()[contains(.,'lift') or contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//div[@id='features']//text()[contains(.,'terrace') or contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        swimming_pool = response.xpath("//div[@id='features']//text()[contains(.,'Pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        item_loader.add_value("external_id", response.url.split("property/")[1].split("/")[0])
        
        item_loader.add_value("landlord_name", "TRUEPENNY'S")
        item_loader.add_value("landlord_phone", "44 (0)20 8693 2277")
        item_loader.add_value("landlord_email", "info@truepennys.com")
        
        
        yield item_loader.load_item()