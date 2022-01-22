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
    name = 'shoulers_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.shoulers.co.uk/properties/lettings/tag-flat",
                    "https://www.shoulers.co.uk/properties/lettings/tag-maisonette",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.shoulers.co.uk/properties/lettings/tag-bungalows",
                    "https://www.shoulers.co.uk/properties/lettings/tag-detached",
                    "https://www.shoulers.co.uk/properties/lettings/tag-house",
                    "https://www.shoulers.co.uk/properties/lettings/tag-semi-detached",
                    "https://www.shoulers.co.uk/properties/lettings/tag-residential"
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//figure"):
            status = item.xpath("./span/text()").get()
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url + f"/page-{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})     
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_id", response.url.split("?")[0].split("properties/")[1].split("/")[0].strip())

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Shoulers_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1[@class='property-name']/text()[1]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='shorten_description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'sq ft')]/text()", input_type="F_XPATH", get_num=True, sq_ft=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//script[contains(.,'saved_properties()')]/text()", input_type="F_XPATH", get_num=True, split_list={'"bedrooms":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//text()[contains(.,'AVAILABLE NOW')]", input_type="F_XPATH", replace_list={"AVAILABLE":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//text()[contains(.,'DEPOSIT:')]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='property-gallery']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'saved_properties()')]/text()", input_type="F_XPATH", split_list={'"lat":':1, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'saved_properties()')]/text()", input_type="F_XPATH", split_list={'"lng":':1, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'PARKING') or contains(.,'Parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Shouler & Son", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01664 560 181", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="lettings@shoulers.co.uk", input_type="VALUE")

        item_loader.add_value("city", item_loader.get_collected_values("address")[0].split(',')[-1].strip())

        rent = response.xpath("//script[contains(.,'saved_properties()')]/text()").get()
        if rent:
            rent = rent.split('"price":"')[1].split('"')[0]
            if 'pa' in rent: item_loader.add_value("rent", str(int(int("".join(filter(str.isnumeric, rent))) / 12)))
            if 'pcm' in rent: item_loader.add_value("rent", "".join(filter(str.isnumeric, rent)))
            item_loader.add_value("currency", 'GBP')

        yield item_loader.load_item()