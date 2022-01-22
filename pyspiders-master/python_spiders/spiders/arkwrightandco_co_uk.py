# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'arkwrightandco_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.arkwrightandco.co.uk/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_division=residential&p_department=RL&location=&propertyType=28&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&orderBy=price%2Bdesc&recentlyAdded=&availability=0%2C1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.arkwrightandco.co.uk/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_division=residential&p_department=RL&location=&propertyType=4%2C23&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&orderBy=price%2Bdesc&recentlyAdded=&availability=0%2C1",
                    "https://www.arkwrightandco.co.uk/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_division=residential&p_department=RL&location=&propertyType=43&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&orderBy=price%2Bdesc&recentlyAdded=&availability=0%2C1"
                ],
                "property_type" : "house"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='search-results-gallery-property']"):
            url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Arkwrightandco_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = response.xpath("//h5//text()").get()
        if title:
            address = title.split("rent in")[1].strip()
            if "," in address:
                city = address.split(",")[-1].strip()
                item_loader.add_value("city", city)
            item_loader.add_value("title", title)
            item_loader.add_value("address", address)
        zipcode=response.url
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("/")[-2].upper().replace("-"," "))

        rent = response.xpath("//p[contains(@class,'price')]/text()").get()
        if rent:
            rent = rent.strip().replace("£","").replace("pcm","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//p[contains(@class,'main_summary')]//text() | //div[@id='full-description']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//h4[contains(.,'Bedroom')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//h4[contains(.,'Key Features:')]//following-sibling::ul//li[contains(.,'bedroom')]//text()").get()
            if room_count:
                room_count = room_count.split("bedroom")[0].strip().split(" ")[-1]
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//h4[contains(.,'Key Features:')]//following-sibling::ul//li[contains(.,'bathroom')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split("bathroom")[0].strip().split(" ")[-1]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'thumbnail_images')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//img[contains(@alt,'floorplan')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        parking = response.xpath("//h4[contains(.,'Key Features:')]//following-sibling::ul//li[contains(.,'parking') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "Arkwright & Co")
        item_loader.add_value("landlord_phone", "01799 668 600")
        item_loader.add_value("landlord_email", "info@arkwrightandco.co.uk")

        yield item_loader.load_item()