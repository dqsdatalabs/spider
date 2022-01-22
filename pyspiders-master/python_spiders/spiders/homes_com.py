# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'homes_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.homes.com/apartments-for-rent/",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.homes.com/houses-for-rent/"
                ],
                "property_type": "house"
            }
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
        
        for item in response.xpath("//a[@data-testid='addr-link']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("id-")[1].split("/")[0])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Homes_PySpider_united_kingdom")
        
        item_loader.add_css("title", "title")
        
        rent = response.xpath("//div[contains(@class,'price')]/text()[contains(.,'$')]").get()
        if rent:
            rent = rent.split("$")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")
        
        room_count = response.xpath("//span[contains(@data-testid,'Bed')]/text()").get()
        if room_count and room_count.strip().isdigit():
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//span[contains(@data-testid,'Bath')]/text()").get()
        if bathroom_count and bathroom_count.isdigit():
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//span[contains(@data-testid,'Sqft')]/text()").get()
        if square_meters and "-" not in square_meters:
            square_meters = str(int(int(square_meters.replace(",",""))* 0.09290304))
            item_loader.add_value("square_meters", square_meters)
                
        description = " ".join(response.xpath("//p[contains(@class,'description')]//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        elevator = response.xpath("//li[contains(.,'Elevator')]//text()[contains(.,'Yes')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        parking = response.xpath("//li[contains(.,'Parking')]//text()[contains(.,'Yes')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        pets_allowed = response.xpath("//li[contains(.,'Pet')]//text()[contains(.,'Yes')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        swimming_pool = response.xpath("//li[contains(.,'Pool')]//text()[contains(.,'Yes')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        images = [x.split("(")[1].split(")")[0] for x in response.xpath("//div[contains(@class,'lzy-img')]/@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        lat_lng = response.xpath("//script[contains(.,'longitude')]/text()").get()
        if lat_lng:
            data = json.loads(lat_lng)
            city = data["address"]["addressLocality"]
            state = data["address"]["addressRegion"]
            zipcode = data["address"]["postalCode"]
            street = ""
            if "streetAddress" in data:
                street = data["address"]["streetAddress"]
            item_loader.add_value("address", f"{city} {street} {state} {zipcode}".strip())
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", f"{state} {zipcode}".strip())
            
            lat = data["geo"]["latitude"]
            lng = data["geo"]["longitude"]
            if lat:
                item_loader.add_value("latitude", str(lat))
            if lng:
                item_loader.add_value("longitude", str(lng))
            
        item_loader.add_value("landlord_name", "Homes.com")
        
        yield item_loader.load_item()