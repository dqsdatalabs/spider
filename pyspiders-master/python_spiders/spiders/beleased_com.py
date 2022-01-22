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
    name = 'beleased_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://beleased.com/search/66561",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://beleased.com/search/66564",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://beleased.com/search/66565",
                ],
                "property_type" : "studio"
            },
            {
                "url" : [
                    "https://beleased.com/search/66566",
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

        for item in response.xpath("//ul[@id='list']/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Beleased_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//ul[@class='o-list']/li[contains(.,'Bedroom')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//ul[@class='o-list']/li[contains(.,'Bathroom')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//span[contains(.,'Date')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@id='gallery']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat:":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng:":1,"}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//ul[@class='o-list']/li[contains(.,'Furnished') or contains(.,' furnished')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//ul[@class='o-list']/li[contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[contains(@class,'prop-desc')]//text()[contains(.,'Balcony')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//div[contains(@class,'prop-desc')]//text()[contains(.,'Washing')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//div[contains(@class,'prop-desc')]//text()[contains(.,'Dish')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//span[contains(.,'Terrace')]/text()", input_type="F_XPATH", tf_item=True)
        
        rent = response.xpath("//span[@class='guide-price']/text()").get()
        if rent:
            rent = rent.split("pm")[0].split("£")[1]
            item_loader.add_value("rent", rent)
        
        deposit = response.xpath("//span[contains(.,'Deposit ')]/text()").get()
        if deposit:
            deposit = deposit.split("month")[0].strip().split(" ")[-1]
            if "one" in deposit:
                item_loader.add_value("deposit", rent)
        
        energy_label = response.xpath("//span[contains(.,'EPC')]/a/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        desc = " ".join(response.xpath("//div[contains(@class,'prop-desc')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="BeLeased", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="028 9122 7789", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@beleased.com", input_type="VALUE")

        yield item_loader.load_item()