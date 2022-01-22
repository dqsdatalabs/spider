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
    name = 'timeconti_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.timeconti.com.au/residential/properties-available?category=Apartment",
                    "https://www.timeconti.com.au/residential/properties-available?category=DuplexSemi-detached",
                    "https://www.timeconti.com.au/residential/properties-available?category=Unit"
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.timeconti.com.au/residential/properties-available?category=Townhouse",
                    "https://www.timeconti.com.au/residential/properties-available?category=House",
                    "https://www.timeconti.com.au/residential/properties-available?category=Villa",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.timeconti.com.au/residential/properties-available?category=Studio"
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(.,'View details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Timeconti_Com_PySpider_australia")          
        item_loader.add_xpath("title","//title/text()")
        item_loader.add_xpath("room_count", "//span[i[@class='fas fa-bed']]/text()")
        item_loader.add_xpath("bathroom_count", "//span[i[@class='fas fa-bath']]/text()")
        rent = "".join(response.xpath("//div[@class='ppcos']/b/text()").getall())
        if rent:
            rent = rent.split("$")[1].lower().split("/")[0].strip().replace(',', '')
            item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'USD')
 
        address = response.xpath("//p[@class='fontsize24']//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split(",")[-1].strip()
            if " Park" not in city:
                item_loader.add_value("city", city.strip()) 
  
        balcony = response.xpath("//p[@class='blocklinks']/span[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True) 
        terrace = response.xpath("//p[@class='blocklinks']/span[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True) 
        parking = response.xpath("//span[i[@class='fas fa-car']]/text()").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
        swimming_pool = response.xpath("//p[@class='blocklinks']/span[.='Pool']/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True) 
        available_date = response.xpath("//p[b[.='Available from:']]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        description = " ".join(response.xpath("//p[@class='propdescrip']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [x for x in response.xpath("//section[@id='propimages']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
      
        item_loader.add_xpath("landlord_name", "//p[@class='ppagndet']/span[i[contains(@class,'fa-user')]]/text()")
        item_loader.add_xpath("landlord_phone", "//p[@class='ppagndet']/span[i[contains(@class,'fa-mobile-alt')]]/text()")
        item_loader.add_value("landlord_email", "cknight@timeconti.com.au")

        yield item_loader.load_item()