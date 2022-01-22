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
    name = 'collings_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            { "url" : "https://www.collings.com.au/lease/properties-for-rent/?list=lease&keywords=&property_type=Apartment&bedrooms=&bathrooms=&carspaces=&price_min=0&price_max=20000&sur_suburbs=1","property_type" : "apartment"},
            { "url" : "https://www.collings.com.au/lease/properties-for-rent/?list=lease&keywords=&property_type=House&bedrooms=&bathrooms=&carspaces=&price_min=0&price_max=20000&sur_suburbs=1","property_type" : "house"},
            { "url" : "https://www.collings.com.au/lease/properties-for-rent/?list=lease&keywords=&property_type=Townhouse&bedrooms=&bathrooms=&carspaces=&price_min=0&price_max=20000&sur_suburbs=1", "property_type" : "house"},
            { "url" : "https://www.collings.com.au/lease/properties-for-rent/?list=lease&keywords=&property_type=Villa&bedrooms=&bathrooms=&carspaces=&price_min=0&price_max=20000&sur_suburbs=1", "property_type" : "house"},
        ]
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='listing']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Collings_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@id='property-description']/h2/text()", input_type="F_XPATH")
        
        address = " ".join(response.xpath("//div[contains(@class,'address')]//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
        
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[@class='suburb']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//i[contains(@class,'bed')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//i[contains(@class,'bath')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="USD", input_type="VALUE")

        price = response.xpath("substring-before(substring-after(//div[@class='price']/text(),'$'),' ')").get()
        if price:
            price = price.replace(",","").split("-")[0]
            item_loader.add_value("rent", int(float(price))*4)
        
        desc = " ".join(response.xpath("//div[@class='copy']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        not_list = ["well", "polis", "incl", "ampl", "with", "open", "dual", "through"]
        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            status = True
            for i in not_list:
                if i in floor:
                    status = False
            if status:
                item_loader.add_value("floor", floor)

        item_loader.add_value("external_id", response.url.split("-")[-1].split("/")[0])
        
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//i[contains(@class,'car')]/following-sibling::text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//p[@class='mfp-gallery']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//a[@title='Floorplan']/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'latitude')]/text()", input_type="F_XPATH", split_list={'latitude":':1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'longitude')]/text()", input_type="F_XPATH", split_list={'longitude":':1, "}":0}, replace_list={"\r\n":""})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//p[@class='name']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//p[contains(@class,'mobile')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="northcote@collings.com.au", input_type="VALUE")
        
        yield item_loader.load_item()