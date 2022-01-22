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
    name = 'piney_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Piney_Co_PySpider_united_kingdom"
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "http://www.piney.co.uk/property-for-rent?q=&st=rent&lp=0&up=0&beds=&radius=0&town=&sta=5&sta=6&sta=7&sty=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.piney.co.uk/property-for-rent?q=&st=rent&lp=0&up=0&beds=&radius=0&town=&sta=5&sta=6&sta=7&sty=9&sty=8&sty=7&sty=6&sty=5&sty=4&sty=3&sty=2&sty=10",
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

        for item in response.xpath("//a[@class='xPL_more']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(.,'NEXT')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        from python_spiders.helper import ItemClear
        item_loader.add_value("external_source",self.external_source)
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'xPP_val xPP_propertyRent')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(@class,'xPP_val xPP_bedrooms')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(@class,'xPP_val xPP_bathrooms')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(@class,'xPP_val xPP_deposit')]/text()", input_type="F_XPATH", get_num=True, replace_list={"£":""})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//span[contains(@class,'xPP_val xPP_available')]/text()", input_type="F_XPATH")
        
        energy_label = response.xpath("//span[contains(@class,'xPP_val xPP_epc')]/a/text()").get()
        if energy_label and "/" in energy_label:
            item_loader.add_value("energy_label", energy_label.split("/")[0])
        
        desc = " ".join(response.xpath("//div[contains(@class,'xPP_attributesDescription')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//span[contains(@class,'xPP_val xPP_furnished')]/text()[contains(.,' furnished') or contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//span[contains(@class,'xPP_val xPP_style')]/text()[contains(.,' Terrace') or contains(.,'terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[contains(@class,'description')]//*[self::p or self::ul]//text()[contains(.,'Parking')][not(contains(.,'No'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'jcarousel-stage')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Piney - Belfast's Number 1 Lettings Agent", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="028 9066 6966", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@piney.co.uk", input_type="VALUE")

        yield item_loader.load_item()