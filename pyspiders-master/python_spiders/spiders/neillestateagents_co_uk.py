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
    name = 'neillestateagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://neillestateagents.co.uk/properties/search?keywords=&address=&type=RESIDENTIAL&excludelet=&minsale=0&maxsale=0&salerent=RENT&propertytype=Apartment&minbedrooms=0&minreceptions=0&minbathrooms=0&rentminimum=0&rentmaximum=0&furnishing=0",
                    "https://neillestateagents.co.uk/properties/search?keywords=&address=&type=RESIDENTIAL&excludelet=&minsale=0&maxsale=0&salerent=RENT&propertytype=Flat&minbedrooms=0&minreceptions=0&minbathrooms=0&rentminimum=0&rentmaximum=0&furnishing=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://neillestateagents.co.uk/properties/search?keywords=&address=&type=RESIDENTIAL&excludelet=&minsale=0&maxsale=0&salerent=RENT&propertytype=Detached&minbedrooms=0&minreceptions=0&minbathrooms=0&rentminimum=0&rentmaximum=0&furnishing=0",
                    "https://neillestateagents.co.uk/properties/search?keywords=&address=&type=RESIDENTIAL&excludelet=&minsale=0&maxsale=0&salerent=RENT&propertytype=Bungalow&minbedrooms=0&minreceptions=0&minbathrooms=0&rentminimum=0&rentmaximum=0&furnishing=0",
                    "https://neillestateagents.co.uk/properties/search?keywords=&address=&type=RESIDENTIAL&excludelet=&minsale=0&maxsale=0&salerent=RENT&propertytype=Semi-Detached+Bungalow&minbedrooms=0&minreceptions=0&minbathrooms=0&rentminimum=0&rentmaximum=0&furnishing=0",
                    "https://neillestateagents.co.uk/properties/search?keywords=&address=&type=RESIDENTIAL&excludelet=&minsale=0&maxsale=0&salerent=RENT&propertytype=Semi-Detached&minbedrooms=0&minreceptions=0&minbathrooms=0&rentminimum=0&rentmaximum=0&furnishing=0",
                    "https://neillestateagents.co.uk/properties/search?keywords=&address=&type=RESIDENTIAL&excludelet=&minsale=0&maxsale=0&salerent=RENT&propertytype=Terrace&minbedrooms=0&minreceptions=0&minbathrooms=0&rentminimum=0&rentmaximum=0&furnishing=0",
                    "https://neillestateagents.co.uk/properties/search?keywords=&address=&type=RESIDENTIAL&excludelet=&minsale=0&maxsale=0&salerent=RENT&propertytype=Townhouse&minbedrooms=0&minreceptions=0&minbathrooms=0&rentminimum=0&rentmaximum=0&furnishing=0",
                    "https://neillestateagents.co.uk/properties/search?keywords=&address=&type=RESIDENTIAL&excludelet=&minsale=0&maxsale=0&salerent=RENT&propertytype=End+Terrace&minbedrooms=0&minreceptions=0&minbathrooms=0&rentminimum=0&rentmaximum=0&furnishing=0",
                    "https://neillestateagents.co.uk/properties/search?keywords=&address=&type=RESIDENTIAL&excludelet=&minsale=0&maxsale=0&salerent=RENT&propertytype=Villa&minbedrooms=0&minreceptions=0&minbathrooms=0&rentminimum=0&rentmaximum=0&furnishing=0",
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

        for item in response.xpath("//div[contains(@class,' property')]//a[@class='more']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@rel='next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Neillestateagents_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='field']/div[contains(.,'Address')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='field']/div[contains(.,'Address')]/following-sibling::div/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@class='field']/div[contains(.,'Address')]/following-sibling::div/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='field']/div[contains(.,'Price')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True, split_list={"£":1, " ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,' furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrace') or contains(.,' terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat:":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng:":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@id='image-gallery']//@src", input_type="M_XPATH")
                    
        energy_label = response.xpath("//img/@src[contains(.,'epc')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("/")[-2])
        
        desc = " ".join(response.xpath("//div[@id='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if response.xpath("//div[@class='field']/div[contains(.,'Bedroom')]/following-sibling::div/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='field']/div[contains(.,'Bedroom')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        elif "bedroom" in desc.lower():
            item_loader.add_value("room_count", desc.split("bedroom")[0].strip().split(" ")[-1])
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]/text()").get()
        if available_date:
            if "immediately" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))

        
        item_loader.add_value("landlord_name", "Neill Estate Agents")
        item_loader.add_value("landlord_phone", "02891814511")
        item_loader.add_value("landlord_email", "ards@neillestateagents.co.uk")

        yield item_loader.load_item()