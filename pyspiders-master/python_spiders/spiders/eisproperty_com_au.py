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
import re

class MySpider(Spider):
    name = 'eisproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.eisproperty.com.au/listings/?keyword=&listing-category=for-rent&location=&listing-type=apartment&min=&max=",
                    "https://www.eisproperty.com.au/listings/?keyword=&listing-category=for-rent&location=&listing-type=unit&min=&max=",
                    "https://www.eisproperty.com.au/listings/?keyword=&listing-category=for-rent&location=&listing-type=flat&min=&max="
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.eisproperty.com.au/listings/?keyword=&listing-category=for-rent&location=&listing-type=house&min=&max=",
                    "https://www.eisproperty.com.au/listings/?keyword=&listing-category=for-rent&location=&listing-type=townhouse&min=&max=",
                    "https://www.eisproperty.com.au/listings/?keyword=&listing-category=for-rent&location=&listing-type=villa&min=&max=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.eisproperty.com.au/listings/?keyword=&listing-category=for-rent&location=&listing-type=studio&min=&max=",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//h3/a[@rel='bookmark']"):
            follow_url = item.xpath("./@href").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Eisproperty_Com_PySpider_australia")

        external_id = response.xpath("//div[contains(@class,'price')]//div[contains(@class,'id')]//text()").get()
        item_loader.add_value("external_id",external_id.strip())
        dontallow=response.xpath("//h1[@class='entry-title']/text()").get()
        if dontallow:
            dontallow=dontallow.lower()
            if "short term" in dontallow:
                return 

        title = response.xpath("//title//text()").get()
        item_loader.add_value("title", title)
                
        rent = response.xpath("//div[contains(@class,'price')]//span[contains(@class,'price')]//text()").get()
        if rent:
            price = rent.split("$")[1].strip().split(" ")[0]
            item_loader.add_value("rent", int(price)*4)        
        item_loader.add_value("currency", "AUD")

        furnished = "".join(response.xpath("//div[contains(@itemprop,'description')]//p[2]//text()").getall())
        if furnished and " furnished" in furnished:
            item_loader.add_value("furnished", True)
        else:            
            if "furnished" in title.lower() or " furnished" in rent.lower():
                item_loader.add_value("furnished", True)

        address = response.xpath("//span[contains(@class,'address')]//text()").get()
        if address:
            zipcode = address.strip().split(" ")[-1]
            city = address.split(",")[1].split("TAS")[0]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        room_count = response.xpath("//span[contains(@title,'Bed')]//span[contains(@class,'value')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(@title,'Bath')]//span[contains(@class,'value')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//span[contains(@title,'Park')]//span[contains(@class,'value')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        desc = " ".join(response.xpath("//div[contains(@itemprop,'description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        images = [x for x in response.xpath("//span[contains(@class,'image')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        landlord_name = "".join(response.xpath("//div[contains(@class,'agent-name')]/text()").getall())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())

        landlord_phone = response.xpath("//span[contains(@class,'agent-phone')]//a//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        landlord_email = "".join(response.xpath("//div[@class='wpsight-listing-agent-name']//comment()").extract())
        if landlord_email:
            landlord_email=landlord_email.split("mailto:")[-1].split('"')[0]
            item_loader.add_value("landlord_email", landlord_email)
    
        yield item_loader.load_item()