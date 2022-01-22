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
from python_spiders.helper import ItemClear

class MySpider(Spider):
    name = 'bt1apartments_co_uk'
    execution_type='testing'
    country='ireland'
    locale='en'
    
    custom_settings = {
        "PROXY_ON": "True"
    }
    def start_requests(self):
        start_url = "https://bt1apartments.co.uk/apartments/"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//article/section"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'More information')]/@href").get())
            rent = item.xpath(".//div[@class='price']/span/text()").get()
            rent = "".join(filter(str.isnumeric, rent)) if rent else rent
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":"apartment", "rent":rent})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)

        rent = response.meta["rent"] # fiyat gecelik olarak geliyor bunu aylığa dönüştürmeniz gerek
        item_loader.add_value("rent", int(rent)*30)
        item_loader.add_value("currency", "GBP")
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Bt1apartments_Co_PySpider_ireland", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='map--lazy']/@data-src", input_type="F_XPATH", split_list={"?q=":1,"&key":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='map--lazy']/@data-src", input_type="F_XPATH", split_list={"?q=":1,"&key":0, ",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@class='map--lazy']/@data-src", input_type="F_XPATH", split_list={"?q=":1,"&key":0, ",":-1})
        
        desc = " ".join(response.xpath("//section[@id='apartment-info']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        bathroom_count = response.xpath("//ul/li[contains(.,'bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("bathroom")[0].strip().split(" ")[-1])
        from word2number import w2n
        if "bedroom" in desc:
            room_count = desc.split("bedroom")[0].strip().split(" ")[-1]
            item_loader.add_value("room_count", w2n.word_to_num(room_count))
            
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'cell-with-bg')]//@data-flickity-lazyload", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'balcony')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'furnish')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'Dishwasher')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//li[contains(.,'Washing')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="BTONE APARTMENTS", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="447821339498", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="enquiry@bt1apartments.co.uk", input_type="VALUE")

        yield item_loader.load_item()