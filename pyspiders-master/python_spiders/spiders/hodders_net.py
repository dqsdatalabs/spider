# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n
import re
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'hodders_net'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.hodders.net/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_department=RL&propertyAge=&national=false&location=&propertyType=8%2C11%2C28&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.hodders.net/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_department=RL&propertyAge=&national=false&location=&propertyType=1%2C3%2C4%2C26&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.hodders.net/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_department=RL&propertyAge=&national=false&location=&propertyType=12&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=",
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
        for item in response.xpath("//div[@id='availablity-1']//div[@class='textHolder']/../@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[.='›']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Hodders_PySpider_united_kingdom")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", title.strip())

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1]
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("pcm")[0].split("£")[1].replace(",",""))
            item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//span[@class='type']/text()").get()
        if room_count:
            room_count = room_count.split("Bedroom")[0].strip()
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split("Bath")[0].strip()
            try:
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
            except:
                pass
        
        floor = response.xpath("//li[contains(.,'Floor')]/text()").get()
        if floor:
            floor = floor.split("Floor")[0].strip()
            item_loader.add_value("floor", floor)
        
        lat_lng = response.xpath("//div/@data-location").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split(",")[0])
            item_loader.add_value("longitude", lat_lng.split(",")[1])
        
        desc = " ".join(response.xpath("//div[contains(@class,'full_desc')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        available_date = ""
        if "available now" in desc.lower():
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        elif "available" in desc.lower():
            available_date = desc.lower().split("available")[1].split(".")[0].strip()
            if "unfurnished" in available_date:
                available_date = available_date.split("unfurnished")[0].strip()
            if "in" in available_date:
                available_date = available_date.split("in")[1].strip()
            
            if " " in available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        if "EPC Rating" in desc:
            energy_label = desc.split("EPC Rating")[1].split(".")[0].strip()
            item_loader.add_value("energy_label", energy_label)
        
        images = response.xpath("//script[contains(.,'thumbnail_images')]/text()").get()
        if images:
            image = images.split('"image"')
            for i in range(1,len(image)):
                item_loader.add_value("images", image[i].split(':"')[1].split('"')[0])
        
        external_id = response.xpath("//span[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        floor_plan_images = "".join(response.xpath("//div[contains(@class,'floorplan')]/@style").getall())
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images.split("('")[1].split("')")[0])
        
        parking = response.xpath("//li[contains(.,'Garage') or contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        unfurnished = response.xpath("//li[contains(.,'Unfurnished')]/text()").get()
        furnished = response.xpath("//li[contains(.,'Furnished')]/text()").get()
        if unfurnished:
            item_loader.add_value("furnished", False)
        elif furnished:
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        item_loader.add_value("landlord_name", "HODDERS")
        item_loader.add_xpath("landlord_phone", "//p[contains(.,'Tel:')]/strong/text()")
        item_loader.add_xpath("landlord_email", "//a[contains(@href,'mail')]/strong/text()")
        
        yield item_loader.load_item()