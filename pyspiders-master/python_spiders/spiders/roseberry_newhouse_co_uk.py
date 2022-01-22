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
    name = 'roseberry_newhouse_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 

    def start_requests(self):
        start_urls = [
            {"url": "https://www.roseberry-newhouse.co.uk/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_department=RL&propertyAge=&national=false&location=&propertyType=28&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&availability=0%2C1", "property_type": "apartment"},
            {"url": "https://www.roseberry-newhouse.co.uk/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_department=RL&propertyAge=&national=false&location=&propertyType=1%2C3&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&availability=0%2C1", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for url in response.xpath("//div[@class='search-results']//div[@class='search-results-gallery-property']/a[1]/@href").extract():
            yield Request(url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Roseberrynewhouse_Co_PySpider_united_kingdom")

        external_id = response.xpath("//span[contains(.,'Ref')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//h2//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h2//text()").get()
        if address: 
            city = address.split(",")[-1].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())

        rent = response.xpath("//span[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.strip().replace("£","").replace("pcm","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP") 

        deposit =response.xpath("//div[contains(@class,'full_description_large')]/b[.='DEPOSIT' or .='Deposit']/following-sibling::text()").get()
        if deposit:
            deposit = deposit.split("£")[1].split(".")[0].strip()
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'full_description_large')]/text()").getall())
        if desc: 
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        furnished = response.xpath("//div[contains(@class,'full_description_large')]/text()").get()
        if furnished: 
                furnished=furnished.split(".")[-2].strip()
                if "furnished" in furnished.lower():
                    item_loader.add_value("furnished", True)

        room_count = response.xpath("//span[contains(@class,'type')]/text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        
        script_image = response.xpath("//script[contains(.,'propertyDetails2Images')]/text()").get()
        if script_image:
            data = json.loads(script_image.split('property-details-photo-gallery",')[1].split(', ["push"]')[0])
            for image in data:
                item_loader.add_value("images", image['image'])
         
        floor_plan_images = response.xpath("//a[contains(.,'Floorplan')]//@href[contains(.,'FLP')]").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        parking = response.xpath("//div[contains(@class,'full_description_large')]//b[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        latitude_longitude = response.xpath("//div[contains(@id,'googlemapContainer')]//@data-location").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(',')[0]
            longitude = latitude_longitude.split(',')[1].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Roseberry Newhouse")
        item_loader.add_value("landlord_phone", "01642 927288")
        item_loader.add_value("landlord_email", "lettings@roseberrygroup.co.uk")

        yield item_loader.load_item()