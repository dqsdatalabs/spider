# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re


class MySpider(Spider):
    name = 'firstnationalnorth_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ["https://firstnationalnorth.com.au/results"]

    request_list = ["Apartment", "House", "Studio", "Terrace", "Townhouse", "Unit", "Villa"]
    request_type = ["apartment", "house", "studio", "apartment", "house", "house", "house"]
    current_index = 0
    # 1. FOLLOWING
    def parse(self, response):
        token = response.xpath("//meta[@name='csrf-token']/@content").get()
        formdata = {
            "authenticityToken": token,
            "_method": "post",
            "LISTING_SALE_METHOD": "Lease",
            "LISTING_CATEGORY": "Residential",
            "listing_property_type": self.request_list[self.current_index],
            "LISTING_BEDROOMS": "",
            "LISTING_PRICE_FROM": "",
            "LISTING_PRICE_TO": "",
        }

        yield FormRequest(
            "https://firstnationalnorth.com.au/results",
            callback=self.jump,
            formdata=formdata,
            meta={
                "property_type":self.request_type[self.current_index],
            }
        )

    def jump(self, response):

        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//li/a[@class='grid']/@href").getall():
            yield Request(
                response.urljoin(item),
                callback=self.populate_item,
                meta={
                    "property_type":response.meta["property_type"],
                }
            )
            seen = True
        
        if page == 2 or seen:
            token = response.xpath("//meta[@name='csrf-token']/@content").get()
            formdata = {
                "authenticityToken": token,
                "_method": "post",
                "LISTING_SALE_METHOD": "Lease",
                "LISTING_CATEGORY": "Residential",
                "listing_property_type": self.request_list[self.current_index],
                "LISTING_BEDROOMS": "",
                "LISTING_PRICE_FROM": "",
                "LISTING_PRICE_TO": "",
                "pg":str(page),
            }

            yield FormRequest(
                "https://firstnationalnorth.com.au/results",
                callback=self.jump,
                formdata=formdata,
                meta={
                    "page":page+1,
                    "property_type":self.request_type[self.current_index],
                }
            )
        elif self.current_index + 1 < len(self.request_list):
            self.current_index += 1
            yield Request(
                "https://firstnationalnorth.com.au/results",
                dont_filter=True,
                callback=self.parse,
            )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Firstnationalnorth_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@class='description']/h3/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h3[contains(.,'Location')]/following-sibling::h2//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h3[contains(.,'Location')]/following-sibling::h2//text()", input_type="F_XPATH", split_list={",":-1})
        
        rent = response.xpath("//h3[contains(.,'Lease')]/following-sibling::text()[1][contains(.,'$')]").get()
        if rent and "-" in rent:
            ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h3[contains(.,'Lease')]/following-sibling::text()[1][contains(.,'$')]", input_type="F_XPATH", get_num=True, per_week=True, split_list={"-":0})
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h3[contains(.,'Lease')]/following-sibling::text()[1][contains(.,'$')]", input_type="F_XPATH", get_num=True, per_week=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//h3[contains(.,'Bond')]/following-sibling::text()[1]", input_type="F_XPATH", get_num=True, split_list={"$":1}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="normalize-space(//h3[contains(.,'Available')]/following-sibling::text()[1])", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='bbc']//text()[contains(.,'Bed')]", input_type="F_XPATH", get_num=True, split_list={"Bed":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='bbc']//text()[contains(.,'Bed')]", input_type="F_XPATH", get_num=True, split_list={"Bath":0, " ":-1})
        
        desc = " ".join(response.xpath("//div[@class='description']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
            
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[@class='description']/text()[contains(.,'lift') or contains(.,'lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@class='feature-list']//text()[contains(.,'Balcon')] | //div[@class='description']/text()[contains(.,'Balcon')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@class='feature-list']//text()[contains(.,'Terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//div[@class='feature-list']//text()[contains(.,'Dishwasher')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='bbc']//text()[contains(.,'Car')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="//div[@class='description']/text()[contains(.,'Pets')][not(contains(.,'No'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="normalize-space(//h3[contains(.,'Pet')]/following-sibling::text()[1][not(contains(.,'No'))])", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='bxslider']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'latLng')]/text()", input_type="F_XPATH", split_list={"lat:":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'latLng')]/text()", input_type="F_XPATH", split_list={"lng:":1, "}":0})
        item_loader.add_value("external_id", response.url.split("estate/")[1].split("/")[0])
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="FIRST NATIONAL REAL ESTATE", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="(02) 9816 3500", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="sales@firstnationalnorth.com.au", input_type="VALUE")
        
        zipcode = response.xpath("//title//text()").get()
        if zipcode and "NSW" in zipcode:
            zipcode = zipcode.split("NSW")[1].split(",")[0].strip()
            item_loader.add_value("zipcode", "NSW " + zipcode)
            
        status = response.xpath("//h3[contains(.,'Lease')]/following-sibling::text()[1][contains(.,'DEPOSIT TAKEN')]").get()
        if not status:
            yield item_loader.load_item()
