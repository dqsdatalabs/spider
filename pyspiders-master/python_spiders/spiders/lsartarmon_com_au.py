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
    name = 'lsartarmon_com_au_disabled'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://lsre.com.au/artarmon/wp-json/api/listings/all?priceRange=&category=Apartment&limit=18&type=rental&status=current&address=&paged=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://lsre.com.au/artarmon/wp-json/api/listings/all?priceRange=&category=House%2CTownhouse%2CVilla&limit=18&type=rental&status=current&address=&paged=1",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://lsre.com.au/artarmon/wp-json/api/listings/all?priceRange=&category=Studio&limit=18&type=rental&status=current&address=&paged=1",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        if data["status"].upper() == 'SUCCESS':
            seen = True
            for item in data["results"]:           
                yield Request(response.urljoin(item["slug"]), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if page == 2 or seen: 
            yield Request(response.url.split('&paged=')[0] + f"&paged={page}", callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Lsartarmon_Com_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("-")[-1].split("/")[0])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address_street = response.xpath("//span[contains(@itemprop,'streetAddress')]//text()").get()
        address_locality = response.xpath("//span[contains(@itemprop,'addressLocality')]//text()").get()
        address_region = response.xpath("//span[contains(@itemprop,'addressRegion')]//text()").get()
        adress_postcode = response.xpath("//span[contains(@itemprop,'postalCode')]//text()").get()
        if address_street:
            address = address_street + " " + address_locality + " " + address_region + " " + adress_postcode
            item_loader.add_value("address", address)
            item_loader.add_value("city", address_locality)
            item_loader.add_value("zipcode", address_region + " " + adress_postcode)

        rent = response.xpath("//span[contains(@class,'price')]//text()[contains(.,'DEPOSIT RECEIVED')]").get()
        if rent:
            return
        else:
            rent = response.xpath("//span[contains(@class,'price')]//text()").get()
            if rent:
                rent = rent.strip().replace("$","").split(" ")[0]
                if rent.isdigit():
                    item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")

        desc = " ".join(response.xpath("//div[contains(@class,'property-description')]//div[contains(@class,'content-2')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'bed')]//span[contains(@class,'value')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[contains(@class,'bath')]//span[contains(@class,'value')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'slick-image')]//@data-original").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//div[contains(@class,'floorplan-wrapper')]//@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'property-date-available')]//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                available_date = available_date.split("from")[1].split(",")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//div[contains(@class,'car')]//span[contains(@class,'value')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@class,'property-description')]//div[contains(@class,'content-2')]//p[2]//text()[contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(@class,'property-description')]//div[contains(@class,'content-2')]//p//text()[contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        elevator = response.xpath("//div[contains(@class,'property-description')]//div[contains(@class,'content-2')]//p//text()[contains(.,'Lift') or contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
            
        swimming_pool = response.xpath("//div[contains(@class,'property-description')]//div[contains(@class,'content-2')]//p//text()[contains(.,'pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        dishwasher = response.xpath("//li[contains(@class,'dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        else:
            dishwasher = response.xpath("//div[contains(@class,'property-description')]//div[contains(@class,'content-2')]//p//text()[contains(.,'dishwasher')]").get()
            if dishwasher:
                item_loader.add_value("dishwasher", True)
        
        pets_allowed = response.xpath("//div[contains(@class,'property-description')]//div[contains(@class,'content-2')]//p//text()[contains(.,'Pets allowed')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(",")[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@id,'author-bio')]//h3//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//div[contains(@id,'author-bio')]//div[contains(@class,'email')]//span//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        
        landlord_phone = response.xpath("//div[contains(@id,'author-bio')]//div[contains(@class,'phone')]//span//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        
        yield item_loader.load_item()