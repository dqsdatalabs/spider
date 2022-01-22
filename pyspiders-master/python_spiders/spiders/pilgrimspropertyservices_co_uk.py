# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'pilgrimspropertyservices_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-apartment",
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-flat",
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-flat-share",
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-new-apartment",
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-serviced-apartments",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-attached-house",
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-detached",
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-detached-house",
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-house",
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-house-share",
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-maisonette",
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-new-home",
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-semi-detached",
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-studio",
                ],
                "property_type": "studio"
            },
            {
                "url": [
                    "https://www.pilgrimspropertyservices.co.uk/search.ljson?channel=lettings&fragment=tag-student-house-share",
                ],
                "property_type": "student_apartment"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        data = json.loads(response.body)
        
        if data["properties"]:
            for item in data["properties"]:
                follow_url = item["property_url"]
                yield Request(response.urljoin(follow_url), callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "item": item})
                seen = True
        
        if page == 2 or seen:
            url = f"{response.url}/page-{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        data = response.meta.get('item')
        status = data["status"]
        if "to let" in status.lower():
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_source","Pilgrimspropertyservices_Co_PySpider_united_kingdom")

            title = response.xpath("//title/text()").get()
            if title:
                item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
            address = response.xpath("normalize-space(//h1[@class='heading__title']/text())").get()
            if address:
                item_loader.add_value("address", address)
                # item_loader.add_value("city", address.split(",")[-1].strip())
                zipcode = address.split(",")[-1].strip()
                if zipcode.isalpha():
                    item_loader.add_value("city", zipcode)
                else:
                    item_loader.add_value("zipcode", zipcode)
                    item_loader.add_value("city", address.split(",")[-2].strip())
            
            rent = data["price_value"]
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency","GBP")
            
            room_count = data["bedrooms"]
            bathroom_count = data["bathrooms"]
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)

            lat = data["lat"]
            lng = data["lng"]
            item_loader.add_value("latitude", str(lat))
            item_loader.add_value("longitude", str(lng))
            
            external_id = data["property_id"]
            item_loader.add_value("external_id", str(external_id))
            
            description = " ".join(response.xpath("//div[@class='property--content']//p//text()").getall())
            if description:
                item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
            
            import dateparser
            if "available from the" in description:
                available_date = description.split("available from the")[1].split(" on")[0].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                
            balcony = response.xpath("//li[contains(.,'Balcony') or contains(.,'balcony')]").get()
            if balcony:
                item_loader.add_value("balcony", True)
            
            parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking') or contains(.,'Garage')]").get()
            if parking:
                item_loader.add_value("parking", True)
            
            elevator = response.xpath("//li[contains(.,'Lift') or contains(.,'lift')]").get()
            if elevator:
                item_loader.add_value("elevator", True)
            
            furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,' furnished')][not(contains(.,'Un'))]").get()
            if furnished:
                item_loader.add_value("furnished", True)
            
            floor = response.xpath("//ul[@class='bullet']/li[contains(.,'Floor')]/text()").get()
            if floor:
                floor = floor.split("Floor")[0].strip().split(" ")[-1]
                item_loader.add_value("floor", floor)
            
            images = [response.urljoin(x) for x in response.xpath("//div[@class='rsContent']//@src").getall()]
            if images:
                item_loader.add_value("images", images)

            floor_plan_images = [x for x in response.xpath("//img/@src[contains(.,'floorplan')]").getall()]
            if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images)
            
            item_loader.add_value("landlord_name", "Pilgrims Property Services")
            item_loader.add_value("landlord_phone", "01932 348620")
        
            yield item_loader.load_item()