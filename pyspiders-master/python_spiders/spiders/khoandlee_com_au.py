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
    name = 'khoandlee_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.khoandlee.com.au/search-results/?list=lease&property_type%5B0%5D=Apartment&min_price&max_price&bedrooms&bathrooms&carspaces",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.khoandlee.com.au/search-results/?list=lease&property_type%5B%5D=House&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "house",
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'listing-item')]/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        next_page = response.xpath("//div[@id='bottom_pagination']//a[.='Next »']/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        rent = response.xpath("//div[@class='suburb-price']//text()").get()
        if "deposit taken" in rent.lower() or "leased" in rent.lower():
            return
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Khoandlee_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='suburb-address']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='suburb-address']//text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h5[contains(@class,'sub-title')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='detail-description']//span//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//label[contains(.,'Bedroom')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//label[contains(.,'Bathroom')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        
        rent = response.xpath("//div[@class='suburb-price']//text()[contains(.,'$')]").get()
        if rent:
            price = rent.replace(",000","").split("$")[1].strip().split(" ")[0].replace("pw","").replace("-","").replace(",","")
            item_loader.add_value("rent", int(price)*4)
    
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//label[contains(.,'Available')]/following-sibling::div/text()[not(contains(.,'Now'))]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//label[contains(.,'ID')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//a/picture//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@data-class='floorplan']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'L.marker([')]/text()", input_type="F_XPATH", split_list={"L.marker([":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'L.marker([')]/text()", input_type="F_XPATH", split_list={"L.marker([":1,",":1,"]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="//li[contains(.,'Pet Friendly')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,' furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Lift') or contains(.,'lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li[contains(.,'Pool') or contains(.,'pool')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//li[contains(.,'Washing') or contains(.,'washing')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'Dishwasher') or contains(.,'dishwasher')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//strong/p/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//p[contains(@class,'phone')]/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//p[contains(@class,'email')]/a/text()", input_type="F_XPATH")

        status = response.xpath("//label[contains(.,'Contract')]/following-sibling::div/text()").get()
        print(status)
        yield item_loader.load_item()