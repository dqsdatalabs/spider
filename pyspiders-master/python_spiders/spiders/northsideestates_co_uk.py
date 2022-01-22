# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
import re

class MySpider(Spider):
    name = 'northsideestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://northside-estates.co.uk/property-search-2/?status=for-rent&type=flat", "property_type": "apartment"},
	        {"url": "https://northside-estates.co.uk/property-search-2/?status=for-rent&type=house", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for follow_url in response.xpath("//div[contains(@class,'property-items-container')]//article/h4/a/@href").extract():
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Northside_Estates_Co_PySpider_united_kingdom")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//address/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            address = address.replace("UK","").strip().strip(",").split(",")[-1]
            if address.strip().split(" ")[-1].isalpha():
                item_loader.add_value("city", address)
            else:
                city = address.strip().split(" ")[0]
                zipcode = address.split(city)[1].strip()
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
        
        square_meters = response.xpath("//span[contains(@class,'size')]/text()").get()
        if square_meters:
            square_meters = square_meters.replace("\xa0"," ")
            if "ft" in square_meters:
                square_meters = square_meters.strip().split(" ")[0]
                sqm = str(int(int(float(square_meters))* 0.09290304))
                item_loader.add_value("square_meters", sqm)
            elif "m" in square_meters:
                item_loader.add_value("square_meters", square_meters.strip().split(" ")[0])
            else:
                square_meters = square_meters.strip().split(" ")[0]
                sqm = str(int(int(float(square_meters))* 0.09290304))
                item_loader.add_value("square_meters", sqm)
        
        room_count = response.xpath("//span[contains(@class,'bedroom')]/text()").get()
        if room_count:
            room_count = room_count.replace("\xa0"," ").strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)


        external_id = "".join(response.xpath("//span[@title='Property ID']/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        bathroom_count = response.xpath("//span[contains(@class,'bath')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.replace("\xa0"," ").strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = "".join(response.xpath("//h5[@class='price']/span[contains(@class,'price')]/text()").getall())
        if rent:
            if "pw" in rent.lower() or "week" in rent.lower():
                price = rent.strip().split(" ")[0].split("£")[1]
                item_loader.add_value("rent", str(int(price)*4))
            else:
                price = rent.strip().split(" ")[0].split("£")[1].replace(",","")
                item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        desc = "".join(response.xpath("//article/div[contains(@class,'content')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        energy_label = response.xpath("//ul[@class='energy-class']//li[contains(@class,'current')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        external_id = "".join(response.xpath("//span[contains(@class,'id')]/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        images = [ x for x in response.xpath("//ul[@class='slides']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        available_date = response.xpath("//div[@class='features']//li[contains(.,'Available')]//text()").get()
        if available_date:
            if "Immediately" in available_date:
                available_date = datetime.now().strftime("%Y-%m-%d")
                item_loader.add_value("available_date", available_date)
        
        balcony = response.xpath("//div[@class='features']//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//div[@class='features']//li[contains(.,'Garage') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        dishwasher = response.xpath("//div[@class='features']//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath("//div[@class='features']//li[contains(.,'Washing Machine') or contains(.,'Washer')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        unfurnished = response.xpath("//div[@class='features']//li[contains(.,'Unfurnished')]//text()").get()
        furnished = response.xpath("//div[@class='features']//li[contains(.,'furnished') or contains(.,'Furnished')]//text()").get()
        if unfurnished:
            item_loader.add_value("furnished", False)
        elif furnished:
            item_loader.add_value("furnished", True)
        
        swimming_pool = response.xpath("//div[@class='features']//li[contains(.,'Pool')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        lat_lng = response.xpath("//script[contains(.,'lng')]//text()").get()
        if lat_lng:
            latitude = lat_lng.split('lat":"')[1].split('"')[0]
            longitude = lat_lng.split('lng":"')[1].split('"')[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "NORTHSIDE ESTATES")
        item_loader.add_value("landlord_phone", "44 (0) 203 973 0656")
        item_loader.add_value("landlord_email", "info@northside-estates.co.uk")
        
        yield item_loader.load_item()