# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'maxwellsestates_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://maxwellsestates.com/property-search/?status=lettings&type=flat",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://maxwellsestates.com/property-search/?status=lettings&type=house",
                    
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
        for item in response.xpath("//h4/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//a[.='Next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})    
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_css("title", "title")
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Maxwellsestates_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//address//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//article/div[contains(@class,'content')]//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div/span[contains(.,'Bedroom')]//text()", input_type="M_XPATH", split_list={"\xa0":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div/span[contains(.,'Bathroom')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slides']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//article/div[contains(@class,'content')]//p//text()[contains(.,'floor')]", input_type="M_XPATH", split_list={"floor":0, " ":-1}, lower_or_upper=1)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lat":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lng":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@class='features']//li[contains(.,'Balcony')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') and not(contains(.,'Un-Furnished'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Russell & Butler Lettings", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0207 22 66 888", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@maxwellsestates.com", input_type="VALUE")

        rent = "".join(response.xpath("//h5/span[contains(@class,'price')]/text()[contains(.,'£')]").getall())
        if rent:
            rent = rent.split("PCM")[0].split("£")[1].replace(",","")
            item_loader.add_value("rent", rent)
            
        
        yield item_loader.load_item()