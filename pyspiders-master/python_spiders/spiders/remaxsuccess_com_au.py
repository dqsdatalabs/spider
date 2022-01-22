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
    name = 'remaxsuccess_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    custom_settings = {
        "PROXY_TR_ON": True,
    }
    external_source = "Remaxsuccess_Com_PySpider_australia"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://remaxsuccess.com.au/wp-json/api/listings/all?agent=&category=Unit&priceRange=&suburb=&bed=0&bath=0&car=0&pet=&address=&sort=&status=current&type=rental",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://remaxsuccess.com.au/wp-json/api/listings/all?agent=&category=Townhouse%2CHouse&priceRange=&suburb=&bed=0&bath=0&car=0&pet=&address=&sort=&status=current&type=rental",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        for item in data["results"]:
            yield Request(response.urljoin(item["slug"]), callback=self.populate_item, meta={"property_type":response.meta["property_type"], "item":item})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item = response.meta.get('item')
        item_loader.add_value("title", item["title"])
        item_loader.add_value("room_count", item["propertyBed"])
        item_loader.add_value("bathroom_count", item["propertyBath"])
        
        address = "".join(response.xpath("//div[@class='entry-title']//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(" ")[-1])
            city = ""
            if "Street" in address:
                city = address.split("Street")[1].split("Qld")[0].strip()
            elif "Court" in address:
                city = address.split("Court")[1].split("Qld")[0].strip()
            elif "St " in address:
                city = address.split("St ")[1].split("Qld")[0].strip()
            else:
                city = address.split("Qld")[0].strip().split(" ")[-1]
            
            item_loader.add_value("city", city)

        square_meters = response.xpath("//div[contains(@class,'land')]/div[@class='data']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
        
        rent = "".join(response.xpath("//div[contains(@class,'price')]//text()").getall())
        if rent:
            price = rent.split("$")[1].strip().split(" ")[0]
            item_loader.add_value("rent", int(price)*4)
        item_loader.add_value("currency", "AUD")
        
        desc = " ".join(response.xpath("//div[contains(@class,'col_last')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        import dateparser
        available_date = item["date"]
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        item_loader.add_value("external_id", response.url.split("-")[-1].split("/")[0])
        
        floor_plan_images = [x for x in response.xpath("//div[contains(@class,'floorpan')]//@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        balcony = response.xpath("//div[contains(@class,'col_last')]//p//text()[contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = item["propertyParking"]
        if parking and parking != 0:
            item_loader.add_value("parking", True)
        
        washing_machine = response.xpath("//div[contains(@class,'col_last')]//p//text()[contains(.,'washing')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        dishwasher = response.xpath("//div[contains(@class,'col_last')]//p//text()[contains(.,'dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        terrace = response.xpath("//div[contains(@class,'col_last')]//p//text()[contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        try:
            pets_allowed = item["petFriendly"]
            if pets_allowed and "yes" in pets_allowed:
                item_loader.add_value("pets_allowed", True)
        except: pass
        
        images = [x for x in response.xpath("//div[@class='pli-container']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'long')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat":"')[1].split('"')[0]
            longitude = latitude_longitude.split('long":"')[1].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "REMAX SUCCESS")
        item_loader.add_value("landlord_phone", "07 4638 6115")
        item_loader.add_value("landlord_email", "inspections@remaxsuccess.com.au")
        
        yield item_loader.load_item()