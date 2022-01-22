# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'chrismorgan_uk_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.chrismorgan.uk.com/property-for-rent?q=&st=rent&lp=0&up=0&beds=&radius=0&town=&sta=5&sty=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.chrismorgan.uk.com/property-for-rent?q=&st=rent&lp=0&up=0&beds=&radius=0&town=&sta=5&sty=9&sty=8&sty=7&sty=6&sty=5&sty=4&sty=3&sty=2&sty=10",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url["url"]:
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url['property_type']})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='xPL_propertyList']/div[@id and not(.//h2[contains(.,'Let Your Property')])]//a[contains(.,'More details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[contains(@class,'xPL_next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Chrismorgan_Uk_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//span[contains(@class,'xPP_briefText')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(@class,'xPP_bedrooms')][2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(@class,'xPP_bathrooms')][2]/text()", input_type="F_XPATH", get_num=True)
        #ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//span[contains(@class,'xPP_availableFrom')][2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'xPP_propertyRent')][2]/text()[not(contains(.,'POA'))]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(@class,'xPP_deposit')][2]/text()", input_type="F_XPATH", get_num=True )
        
        item_loader.add_value("external_id", response.url.split("/")[-1])
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        furnished = response.xpath("//span[contains(@class,'xPP_furnished')][2]/text()[contains(.,'furnished') or contains(.,'Furnished')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        available_date = response.xpath("//span[contains(@class,'xPP_availableFrom')][2]/text()").get()
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date)
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='xPhotoViewer_thumbs']/a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Chris Morgan Estate Agents", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="028 8772 7897", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="chris@chrismorgan.uk.com", input_type="VALUE")

        yield item_loader.load_item()