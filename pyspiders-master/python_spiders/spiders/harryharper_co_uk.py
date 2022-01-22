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
    name = 'harryharper_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://harryharper.co.uk/property-to-rent/apartment/any-bed/all-location?exclude=1",
                    "https://harryharper.co.uk/property-to-rent/flat/any-bed/all-location?exclude=1",

                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://harryharper.co.uk/property-to-rent/detached%20house/any-bed/all-location?exclude=1",
                    "https://harryharper.co.uk/property-to-rent/end%20of%20terrace%20house/any-bed/all-location?exclude=1",
                    "https://harryharper.co.uk/property-to-rent/house/any-bed/all-location?exclude=1",
                    "https://harryharper.co.uk/property-to-rent/semi-detached%20bungalow/any-bed/all-location?exclude=1",
                    "https://harryharper.co.uk/property-to-rent/semi-detached%20house/any-bed/all-location?exclude=1",
                    "https://harryharper.co.uk/property-to-rent/terraced%20house/any-bed/all-location?exclude=1"
                    
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://harryharper.co.uk/property-to-rent/student-property/any-bed/all-location?exclude=1",

                ],
                "property_type" : "student_apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='card']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )  
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Harryharper_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"/":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[@class='displayname']/text()", input_type="F_XPATH", split_list={" in ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[@class='displayname']/text()", input_type="F_XPATH", split_list={" in ":-1, ",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[@class='displayname']/text()", input_type="F_XPATH", split_list={" in ":-1, ",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='desc']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//section[@class='gallery']//i[contains(@class,'fa-bed')]/../../p/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//section[@class='gallery']//i[contains(@class,'fa-bath')]/../../p/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//section[@class='gallery']//span[@class='price-value']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//text()[contains(.,'Deposit ')]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@id='PropertyGallery']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//h3[contains(.,'Floorplan')]/following-sibling::div/img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div[@class='mapsEmbed']/iframe/@src", input_type="F_XPATH", split_list={"?q=":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div[@class='mapsEmbed']/iframe/@src", input_type="F_XPATH", split_list={"?q=":1, ",":1, "&":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='desc']//text()[contains(.,'Parking') or contains(.,'parking') and contains(.,'|')]", input_type="F_XPATH", tf_item=True)
        #ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='image couch']/following-sibling::p/text()", input_type="F_XPATH", tf_item=True, tf_words={True:'furnished', False:'unfurnished'})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Harry Harper Sales & Lettings", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="02920 310555", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@harryharper.co.uk", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//h3[contains(.,'Energy Performance')]/following-sibling::div/img/@src", input_type="F_XPATH", split_list={"_":-2})

        furnished = response.xpath("//div[@class='image couch']/following-sibling::p/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        
        yield item_loader.load_item()