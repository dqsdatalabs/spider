# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser


class MySpider(Spider):
    name = 'bathstoneproperty_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://bathstoneproperty.com/wp-content/themes/bathstone/alg-search.php?index=active&lat=51.3743247&lng=-2.3668175&radius=16093&bedrooms_min=1&bedrooms_max=99&sales=0&lettings=1&detached=0&semi_detached=0&terraced=0&apartment=1&garden=1&sstc=1&lsta=1&price_min=&price_max=&rent_min=600&rent_max=3000&reception_rooms_min=0&reception_rooms_max=99&bathrooms_min=0&bathrooms_max=99&hmo=1&off_street_parking=0", "property_type": "apartment"},
            {"url": "https://bathstoneproperty.com/wp-content/themes/bathstone/alg-search.php?index=active&lat=51.3743247&lng=-2.3668175&radius=16093&bedrooms_min=1&bedrooms_max=99&sales=0&lettings=1&detached=0&semi_detached=0&terraced=1&apartment=0&garden=1&sstc=1&lsta=1&price_min=&price_max=&rent_min=600&rent_max=3000&reception_rooms_min=0&reception_rooms_max=99&bathrooms_min=0&bathrooms_max=99&hmo=1&off_street_parking=0", "property_type": "apartment"},
	        {"url": "https://bathstoneproperty.com/wp-content/themes/bathstone/alg-search.php?index=active&lat=51.3743247&lng=-2.3668175&radius=16093&bedrooms_min=1&bedrooms_max=99&sales=0&lettings=1&detached=1&semi_detached=0&terraced=0&apartment=0&garden=1&sstc=1&lsta=1&price_min=&price_max=&rent_min=600&rent_max=3000&reception_rooms_min=0&reception_rooms_max=99&bathrooms_min=0&bathrooms_max=99&hmo=1&off_street_parking=0", "property_type": "house"},
            {"url": "https://bathstoneproperty.com/wp-content/themes/bathstone/alg-search.php?index=active&lat=51.3743247&lng=-2.3668175&radius=16093&bedrooms_min=1&bedrooms_max=99&sales=0&lettings=1&detached=0&semi_detached=1&terraced=0&apartment=0&garden=1&sstc=1&lsta=1&price_min=&price_max=&rent_min=600&rent_max=3000&reception_rooms_min=0&reception_rooms_max=99&bathrooms_min=0&bathrooms_max=99&hmo=1&off_street_parking=0", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        data = json.loads(response.body)

        for item in data["active"]:
            follow_url = f"https://bathstoneproperty.com/property/?id={item['objectID']}"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Bathstoneproperty_PySpider_"+ self.country)
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        rent = response.xpath("//strong[@class='text-2xl']/text()").get()
        if rent:
            price = rent.split(" ")[0].split("£")[1].replace(",","")
            item_loader.add_value("rent", price)

        
        item_loader.add_value("currency", "GBP")

        room_count = response.xpath("//span/i[contains(@class,'bed')]/parent::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//span/i[contains(@class,'shower')]/parent::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        available_date = response.xpath("//ul/li[contains(.,'Available')]//text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                        available_date.split("Available")[1].strip(), date_formats=["%m/%Y"]
                    )
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m")
                item_loader.add_value("available_date", date2)
        
        desc = "".join(response.xpath("//div[contains(@class,'2/3')]//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc)
            
        images = [ x for x in response.xpath("//div[contains(@class,'property_gallery')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = response.xpath("//label[contains(.,'Floor Plan')]/../div/img/@src[contains(.,'FLP')]").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        lat_lng = response.xpath("//script[contains(.,'lat:')]//text()").get()
        if lat_lng:
            lat = lat_lng.split("lat:")[1].split(",")[0].strip()
            lng = lat_lng.split("lng:")[1].split("}")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        parking = response.xpath("//ul/li[contains(.,'garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_value("landlord_name", "Bath Stone Property")
        
        phone = response.xpath("//span[contains(.,'Call')]/a/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        item_loader.add_value("landlord_email", "lettings@bathstoneproperty.com")
        
        yield item_loader.load_item()