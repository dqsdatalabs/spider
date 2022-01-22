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
    name = 'vanderhuizen_com'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.vanderhuizen.com/huurwoningen?type%5B%5D=appartement-2-kamer&type%5B%5D=appartement-3-kamer&price_from=0&price_to=&sort=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.vanderhuizen.com/huurwoningen?type%5B%5D=kamer&price_from=0&price_to=&sort=",
                ],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='inner']/h3/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Vanderhuizen_PySpider_netherlands")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)
        
        zipcode = response.xpath("//div[contains(@class,'row')]//div[contains(.,'Adres')]//following-sibling::div[1]/text()").get()
        address = response.xpath("//div[contains(@class,'titlecolumn')]/h2/text()").get()
        if address:
            item_loader.add_value("address", address)
        
        if zipcode:
            city = zipcode.split("(")[1].split(")")[0]
            item_loader.add_value("city", city)
            
            zipcode = zipcode.split(city)[0].replace("(","").strip()
            count = zipcode.count(" ")
            if count == 0:
                item_loader.add_value("zipcode", zipcode)
            elif count ==1:
                item_loader.add_value("zipcode", zipcode.split(" ")[0])
            elif count>1:
                item_loader.add_value("zipcode", zipcode.split(" ")[0]+ zipcode.split(" ")[1])
        
        room_count = response.xpath("//div[contains(@class,'row')]//div[contains(.,'Aantal kamers')]//following-sibling::div[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif "-kamer" in title:
            room_count = title.split("-kamer")[0].split(" ")[-1]
            item_loader.add_value("room_count", room_count)
            
        bathroom_count = response.xpath("//div[contains(@class,'row')]//div[contains(.,'Aantal badkamers')]//following-sibling::div[1]/text()[.!='0']").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        elif response.xpath("//li[contains(.,'Badkamer')]"):
            item_loader.add_value("bathroom_count", "1")
            
        square_meters = response.xpath("//div[contains(@class,'row')]//div[contains(.,'Woonoppervlakte')]//following-sibling::div[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        rent = response.xpath("//div[contains(@class,'row')]//div[contains(.,'Huurprijs')]//following-sibling::div[1]/text()").get()
        price = ""
        if rent:
            price = rent.split(",")[0].split("€")[1].replace(" ","").replace(".","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
            
        utilities = response.xpath("//div[contains(@class,'row')]//div[contains(.,'Kale huurprijs')]//following-sibling::div[1]/text()").get()
        if utilities:
            utilities = utilities.split(",")[0].split("€")[1].replace(" ","").replace(".","")
            utility = int(price)-int(utilities)
            item_loader.add_value("utilities", utility)
        
        floor = response.xpath("//div[contains(@class,'row')]//div[contains(.,'Verdieping')]//following-sibling::div[1]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        available_date = response.xpath("//ul/li[contains(.,'Beschikbaar vanaf')]/var/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        desc = "".join(response.xpath("//div[@class='Text']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        images = [ x for x in response.xpath("//ul[@class='slides']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        deposit = response.xpath("//div[contains(@class,'col-xs')][contains(.,'Borg')]/following-sibling::div[1]/text()").get()
        if deposit:
            if "€" in deposit:
                item_loader.add_value("deposit", deposit.split(",-")[0].split("€")[1].strip())
            elif "eenmaal" in deposit.lower() or "1x" in deposit:
                item_loader.add_value("deposit", int(float(price))*1)
            elif "tweemaal" in deposit.lower():
                item_loader.add_value("deposit", int(float(price))*2)
        
        item_loader.add_value("landlord_name", "VANDER HUIZEN")
        
        yield item_loader.load_item()