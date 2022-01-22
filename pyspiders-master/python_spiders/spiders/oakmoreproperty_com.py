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
    name = 'oakmoreproperty_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "http://www.oakmoreproperty.com/properties?location_txt=&type_sel=Apartment&minprice_sel=&maxprice_sel=&search_btn=Search",
                    "http://www.oakmoreproperty.com/properties?location_txt=&type_sel=Flat&minprice_sel=&maxprice_sel=&search_btn=Search",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.oakmoreproperty.com/properties?location_txt=&type_sel=Detached&minprice_sel=&maxprice_sel=&search_btn=Search",
                    "http://www.oakmoreproperty.com/properties?location_txt=&type_sel=Detached+Bungalow&minprice_sel=&maxprice_sel=&search_btn=Search",
                    "http://www.oakmoreproperty.com/properties?location_txt=&type_sel=End+Terrace&minprice_sel=&maxprice_sel=&search_btn=Search",
                    "http://www.oakmoreproperty.com/properties?location_txt=&type_sel=Semi-Detached&minprice_sel=&maxprice_sel=&search_btn=Search",
                    "http://www.oakmoreproperty.com/properties?location_txt=&type_sel=Terrace&minprice_sel=&maxprice_sel=&search_btn=Search",
                    "http://www.oakmoreproperty.com/properties?location_txt=&type_sel=Townhouse&minprice_sel=&maxprice_sel=&search_btn=Search",
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

        for item in response.xpath("//article[@class='property' and not(img[contains(@alt,'let-agreed')])]/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Oakmoreproperty_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1[@id='pageTitle']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h2[@property='pp:address']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h2[@property='pp:address']/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h2[@property='pp:address']/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li[@class='price']/strong/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[@property='pp:bedrooms']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[@property='pp:bathrooms']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,' park')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'dishwasher')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='slideshow-inner']//@src", input_type="M_XPATH")
        
        energy_label = response.xpath("//li[@property='pp:epc']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("/")[0].strip())
        
        desc = " ".join(response.xpath("//div[@class='description']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        floor = response.xpath("//li[contains(.,'floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("floor")[0].strip().split(" ")[-1])
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Oakmore Property Services", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="028 9068 6733", input_type="VALUE")
        
        yield item_loader.load_item()