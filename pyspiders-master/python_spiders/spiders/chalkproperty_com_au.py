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
    name = 'chalkproperty_com_au'   
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.chalkproperty.com.au/rent/properties-for-rent/search/all/rent?property_type=27",
                    "https://www.chalkproperty.com.au/rent/properties-for-rent/search/all/rent?property_type=2",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.chalkproperty.com.au/rent/properties-for-rent/search/all/rent?property_type=1",
                    "https://www.chalkproperty.com.au/rent/properties-for-rent/search/all/rent?property_type=114",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):

        for item in response.xpath("//div[@class='property_list_item']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Chalkproperty_Com_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = " ".join(response.xpath("//h2[contains(@class,'title')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//p[contains(@class,'address')]//text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//h1//text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        rent = response.xpath("//p[contains(@class,'price')]//text()[contains(.,'LEASE PENDING')]").get()
        if rent:
            return
        else:
            rent = "".join(response.xpath("//p[contains(@class,'price')]//text()").getall())
            if rent:
                rent = rent.split("$")[1].split(" ")[0]
                item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "AUD")

        desc = " ".join(response.xpath("//div[contains(@class,'content')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[contains(@class,'bed')]//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(@class,'bath')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x.split("url('")[1].split("'")[0] for x in response.xpath("//section[contains(@class,'image-slider')]//@style[contains(.,'background')]").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(@class,'car')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        latitude_longitude = response.xpath("//script[contains(.,'latitude')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude":')[1].split(',')[0]
            longitude = latitude_longitude.split('longitude":')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Chalk Property")
        item_loader.add_value("landlord_phone", "(08) 9527 8322")
        item_loader.add_value("landlord_email", "enquiry@chalkproperty.com.au")
 
        yield item_loader.load_item()