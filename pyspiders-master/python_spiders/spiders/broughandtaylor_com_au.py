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
import dateparser

class MySpider(Spider):
    name = 'broughandtaylor_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.broughandtaylor.com.au/rent?search=&listing_type=rent&property_type=Apartment&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.broughandtaylor.com.au/rent?search=&listing_type=rent&property_type=Unit&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.broughandtaylor.com.au/rent?search=&listing_type=rent&property_type=Flat&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.broughandtaylor.com.au/rent?search=&listing_type=rent&property_type=House&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.broughandtaylor.com.au/rent?search=&listing_type=rent&property_type=Townhouse&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.broughandtaylor.com.au/rent?search=&listing_type=rent&property_type=Duplex&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.broughandtaylor.com.au/rent?search=&listing_type=rent&property_type=Semi-detached&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.broughandtaylor.com.au/rent?search=&listing_type=rent&property_type=Terrace&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "http://www.broughandtaylor.com.au/rent?search=&listing_type=rent&property_type=Villa&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                ],
                "property_type" : "house"
            },
                        {
                "url" : [
                    "http://www.broughandtaylor.com.au/rent?search=&listing_type=rent&property_type=Studio&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
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

        for item in response.xpath("//div[@class='card']/div/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rent = "".join(response.xpath("//span[@class='price']/text()[contains(.,'LEASED') or contains(.,'DEPOSIT ')]").extract())
        if rent:
            return
        item_loader.add_value("external_source", "Broughandtaylor_Com_PySpider_australia")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("property_id=")[1].split("/")[0])
        item_loader.add_xpath("title", "//title/text()")

        rent =" ".join(response.xpath("//span[@class='price']/text()[contains(.,'$')]").extract())
        if rent:         
            price =  rent.strip().replace("pw","").strip().split(" ")[0].split("$")[1].replace(",","").strip().replace("per","")
            item_loader.add_value("rent",int(float(price.replace("-","")))*4)
        item_loader.add_value("currency","USD")

        item_loader.add_xpath("bathroom_count", "//ul[@class='details']/li[span[contains(@class,'flaticon-shower')]]/text()")

        address = " ".join(response.xpath("//div[@class='inner_desc']/h3/text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            item_loader.add_value("city",address.split(",")[-1].strip())

        room = ""
        room_count = " ".join(response.xpath("//ul[@class='details']/li[i[contains(@class,'laticon-person')]]/text()").getall())
        if room_count:
            room =  room_count
        elif response.meta.get('property_type') == "studio":
            room = "1"
        item_loader.add_value("room_count",room)

        desc =  " ".join(response.xpath("//div[@class='section prop-desc']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [ x for x in response.xpath("//div[@class='carousel-wrap']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images) 

        lat = "".join(response.xpath("//script/text()[contains(.,'.setView([')]").extract())
        if lat:
            latitude = lat.split(".setView([")[1].split(",")[0]
            longitude = lat.split(".setView([")[1].split(",")[1].split("]")[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        pets_allowed = "".join(response.xpath("//div[@class='description']/p/text()[contains(.,'Pets')]").extract())
        if pets_allowed:
            if "yes" in pets_allowed.lower() :
                item_loader.add_value("pets_allowed", True)
            elif "no" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", False)

        parking = "".join(response.xpath("//ul[@class='details']/li[i[contains(@class,'flaticon-car')]]/text()").extract())      
        if parking:
            (item_loader.add_value("parking", True) if "0" not in parking else item_loader.add_value("parking", False))

        balcony = "".join(response.xpath("//div[@class='description']/p/text()[contains(.,'Balcony')]").extract())      
        if balcony:
            item_loader.add_value("balcony", True) 

        item_loader.add_xpath("landlord_name", "//div[@class='agent-widget']/div[1]//div[@class='title']/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent-widget']/div[1]//li/a[contains(@href,'tel')]/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='agent-widget']/div[1]//li/a[contains(@href,'mail')]/text()")

        

        yield item_loader.load_item()