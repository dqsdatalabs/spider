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
    name = 'professionalproperties_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agentexperts.co.uk/?post_type=property&student=&sales_lettings=Lettings&address=&distance=100000&type=2&property_min_price=0&property_max_price=999999999&property_min_beds=1",
                ],
                "property_type" : "apartment",
            },
            { 
                "url" : [
                    "https://www.agentexperts.co.uk/?post_type=property&student=&sales_lettings=Lettings&address=&distance=100000&type=1&property_min_price=0&property_max_price=999999999&property_min_beds=1",
                    "https://www.agentexperts.co.uk/?post_type=property&student=&sales_lettings=Lettings&address=&distance=100000&type=3&property_min_price=0&property_max_price=999999999&property_min_beds=1",
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
        for item in response.xpath("//div[@class='thumb-holder']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//div[@class='nav-previous']/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0]) 

        from python_spiders.helper import ItemClear 
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agentexperts_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='blog_item']//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[@class='bedrooms']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[@class='bathrooms']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='prices']/text()", per_week=True, input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//a[contains(.,'AVAILABLE')]/text()", input_type="F_XPATH", replace_list={"AVAILABLE":"now"})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='bxslider']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//h4[contains(.,'Floor Plan')]/following-sibling::div[1]//img[@class='floorplans']/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,'furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'garage') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//li[contains(.,'Washing machine') or contains(.,'washing machine')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="substring-after(//span[@class='id']/text(),':')", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Agent Experts", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01332 300100", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="enquiries@professionalproperties.co.uk", input_type="VALUE")

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split(',')[-1].strip().split(' ')[0]
            item_loader.add_value("zipcode", " ".join(address.split(city)[-1].strip().split(" ")[-2:]))
            item_loader.add_value("city", city)

        yield item_loader.load_item() 
        