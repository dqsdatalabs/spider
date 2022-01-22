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
    name = 'gibsonlane_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=flat",
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=apartment",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=bungalow-detached",
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=house-detached",
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=house-end-terrace",
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=house-end-town-house",
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=house-link-detached",
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=house-mid-terrace",
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=house-semi-detached",
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=house-terraced",
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=house-townhouse",
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=maisonette",
                    
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=studio",
                ],
                "property_type" : "studio",
            },
            {
                "url" : [
                    "https://gibsonlane.co.uk/property-search/?status=to-let&type=room",
                ],
                "property_type" : "room",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(""),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//article/h4/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url + f"/page/{page}/"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Gibsonlane_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[@title='Property ID']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//address/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//address/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//address/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='content clearfix']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Bedroom')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'Bathroom')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='price-and-type']/text()[2]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'AVAILABLE')]/a/text()", input_type="F_XPATH", replace_list={"AVAILABLE":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@id,'carousel')]//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@class='floor-plan-map']/a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'\"lat\"')]/text()", input_type="F_XPATH", split_list={'"lat":"':-1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'\"lat\"')]/text()", input_type="F_XPATH", split_list={'"lng":"':-1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Lift') or contains(.,'lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Gibson Lane", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="020 8546 5444", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="kingston@gibsonlane.co.uk", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,'furnished')]", input_type="F_XPATH", tf_item=True, tf_value=True)
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//li[contains(.,'EPC Rating')]/a/text()", input_type="F_XPATH", split_list={"EPC Rating":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//text()[contains(.,'sq ft') or contains(.,'sqft')]", input_type="F_XPATH", get_num=True, sq_ft=True, split_list={"sqft":0, "sq ft":0, " ":-1})
        
        yield item_loader.load_item()