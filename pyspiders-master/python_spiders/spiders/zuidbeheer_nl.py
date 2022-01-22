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
    name = 'zuidbeheer_nl'
    execution_type='testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.zuidbeheer.nl/get-residences/"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                        )
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["residences"]:
            if "rented" not in item["features"]:
                follow_url = item["url"]
                p_type = item["features"]["type"][0]["value"]
                if "house" in p_type or "apartment" in p_type or "room" in p_type:
                    yield Request(follow_url, callback=self.populate_item, meta={"item":item, "property_type":p_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Zuidbeheer_PySpider_netherlands")
        item_loader.add_xpath("title", "//h1/text()")
        
        jresp = response.meta.get("item")
        item_loader.add_value("latitude", jresp["latitude"])
        item_loader.add_value("longitude", jresp["longitude"])
        item_loader.add_value("room_count", jresp["features"]["bedrooms"])
        item_loader.add_value("square_meters", jresp["features"]["surface"])
        item_loader.add_value("rent", jresp["features"]["price"])
        item_loader.add_value("currency", "EUR")
        
        furnished = response.xpath("//div[div[label[contains(.,'Inrichting')]]]/div[2]/text()").extract_first()
        if furnished:
            if "Gemeubileerd" in furnished:
                item_loader.add_value("furnished", True)
        
        address = ", ".join(response.xpath("//div[contains(@class,'d-lg-block')]//div[@class='streetname' or @class='city' ]/text()").extract())
        if address:
            item_loader.add_value("address",re.sub("\s{2,}", " ", address)) 
        city =response.xpath("//div[@class='city']/text()").extract_first()
        if city:
            item_loader.add_value("city",city.strip() ) 
        zipcode =response.xpath("//div[div[label[contains(.,'Postcode')]]]/div[2]/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip().replace("Eindhoven","") ) 
        desc = " ".join(response.xpath("//div/div[contains(@class,'main-info')]/following-sibling::p//text()[not(contains(.,'Specificaties'))]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [x for x in response.xpath("//div[@class='residence-images-print']/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        available_date = response.xpath("substring-after(//li[contains(.,'Beschikbaar: per')]//text(),'Beschikbaar: per ')").extract_first() 
        if not available_date:
            available_date = response.xpath("//div[div[label[contains(.,'Beschikbaar per')]]]/div[2]/text()").extract_first() 
        if available_date:           
            date_parsed = dateparser.parse(available_date, languages=['nl'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        item_loader.add_value("landlord_name", "ZUID BEHEER")
        item_loader.add_value("landlord_phone","0402180071")
        item_loader.add_value("landlord_email","info@zuidbeheer.nl")

        yield item_loader.load_item()