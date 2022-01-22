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
    name = 'pinpointproperty_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://pinpointproperty.com/search/544362/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://pinpointproperty.com/search/544363/",
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

        for item in response.xpath("//ul[@id='list']/li/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//li[@class='next']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[5])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Pinpointproperty_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        zipcode = response.xpath("//h1/text()").get()
        if zipcode:
            zipcode = zipcode.split(",")[-1].strip()
            if " " in zipcode:
                zipcode = f"{zipcode.split(' ')[-2]} {zipcode.split(' ')[-1]}".strip()
                ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value=zipcode, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='textbp']//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/span[contains(.,'Bedroom')]/following-sibling::span//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li/span[contains(.,'Price')]/following-sibling::span/span/text()", input_type="F_XPATH", get_num=True, split_list={"pm":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'Available')]/text()", input_type="F_XPATH", split_list={"Available":1}, replace_list={"early":"", "Early":"", "**NOT HMO**":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@id='gallery']//@src", input_type="M_XPATH")
        
        latitude_longitude = response.xpath("//script[contains(.,'lat:')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat:")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("lng:")[1].split("}")[0].strip()
            if "result" in latitude:
                latitude = latitude_longitude.split("LatLng(")[1].split(",")[0].strip()
                latitude = latitude_longitude.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)
            else:
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)
                
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lat:')]/text()", input_type="F_XPATH", split_list={"lat:":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lat:')]/text()", input_type="F_XPATH", split_list={"lng:":1,"}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(.,'floor ')]/text()", input_type="F_XPATH", split_list={"floor":0, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'parking') or contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'furnished') or contains(.,'Furnished')][not(contains(.,'Un'))]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,' terrace') or contains(.,'Terrace')] | //li/span[contains(.,'Style')]/following-sibling::span[contains(.,'Terrace')]", input_type="F_XPATH", tf_item=True)
       
        energy_label = response.xpath("//li/span[contains(.,'EPC')]/following-sibling::span//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("/")[0])
        else:
            energy_label = response.xpath("//li[contains(.,'EPC')]/text()").get()
            if energy_label:
                item_loader.add_value("energy_label", energy_label.split(":")[1].strip())
            
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="PINPOINT", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="028 9068 2777", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@pinpointproperty.com", input_type="VALUE")

        yield item_loader.load_item()