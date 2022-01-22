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
    name = 'pattinson_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.pattinson.co.uk/rent/property-search/card-results?p={}&IsStudent=False&FurnishRequirement=Any&ExcludeExtendedSearchFilters=False&Radius=1&PropertySort=Recent&PropertyTypes[0]=1&PropertyTypes[1]=7&PropertyTypes[2]=8&PropertyTypes[3]=9&PropertyTypes[4]=14&PropertyTypes[5]=16&OnlineSearch=True&Page=1&PageSize=12&_=1610541403334",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.pattinson.co.uk/rent/property-search/card-results?p={}&IsStudent=False&FurnishRequirement=Any&ExcludeExtendedSearchFilters=False&Radius=1&PropertySort=Recent&PropertyTypes[0]=2&PropertyTypes[1]=11&PropertyTypes[2]=12&PropertyTypes[3]=15&OnlineSearch=True&Page=1&PageSize=12&_=1610541403374",
                    
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.pattinson.co.uk/rent/property-search/card-results?p={}&IsStudent=False&FurnishRequirement=Any&ExcludeExtendedSearchFilters=False&Radius=1&PropertySort=Recent&PropertyTypes[0]=13&OnlineSearch=True&Page=1&PageSize=12&_=1610541320402",
                    
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='stretched-link']"):
            status = item.xpath("./../div[contains(@class,'e-tag')]/@class").get()
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
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
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Pattinson_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"id=":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[@itemprop='streetAddress']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[@itemprop='streetAddress']/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[@itemprop='streetAddress']/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Bedrooms:')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='property-price']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//p[contains(.,'Available From')]/text()", input_type="F_XPATH", lower_or_upper=0, split_list={"from":1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='gallery']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//h4[contains(.,'Floorplan')]/following-sibling::img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//meta[@itemprop='latitude']/@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//meta[@itemprop='longitude']/@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//li[contains(.,'EPC Rating')]/text()", input_type="F_XPATH", lower_or_upper=1, split_list={"RATING":-1, "/":-1}, replace_list={":":"", "-":"", "TBC":""})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcony') or contains(.,'balcony')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Lift') or contains(.,'lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrace') or contains(.,'terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li[contains(.,'Swimming Pool') or contains(.,'swimming pool')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//li[contains(.,'Washing Machine') or contains(.,'Washing machine')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'Dishwasher') or contains(.,'dishwasher')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Pattinson", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//p[contains(.,'Alternatively call')]/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//p[contains(.,'Alternatively call')]/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,'furnished')]", input_type="F_XPATH", tf_item=True, tf_value=True)

        if response.xpath("//h3[contains(.,'Bathroom')]").get(): item_loader.add_value("bathroom_count", '1')

        yield item_loader.load_item()