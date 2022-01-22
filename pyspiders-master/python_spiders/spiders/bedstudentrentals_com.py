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
    name = 'bedstudentrentals_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self):
        formdata = {
            "propsearchtype": "",
            "searchurl": "/",
            "market": "1",
            "ccode": "UK",
            "view": "grid",
            "querytype": "106",
            "postcodes": "",
            "pricerange": "",
            "propbedr": "",
            "tenadate": "",
        }
        url = "https://www.bedstudentrentals.com/results"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 12)
        seen = False
        for item in response.xpath("//div[@class='photo-contain']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 12 or seen:
            p_url = f"https://www.bedstudentrentals.com/results?searchurl=%2f&market=1&ccode=UK&view=grid&querytype=106&offset={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+12}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", "student_apartment")

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Bedstudentrentals_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/a/text()", input_type="F_XPATH", split_list={"-":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/a/text()", input_type="F_XPATH", split_list={"-":-1, ",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='description']//header/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[@class='bedroom']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[@class='bathroom']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='priceask']/text()", input_type="F_XPATH", get_num=True, per_week=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//span[@class='available-date']/text()", input_type="F_XPATH", split_list={"from":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='slides']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'detail?')]/text()", input_type="F_XPATH", split_list={'"latitude":"':-1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'detail?')]/text()", input_type="F_XPATH", split_list={'"longitude":"':-1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[@class='parking']/text()", input_type="F_XPATH", tf_item=True, tf_words={True:"Y", False:"N"})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='detail-label' and contains(.,'Furnished')]/following-sibling::div/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//div[@class='detail-label' and contains(.,'Washing Machine')]/following-sibling::div/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Bed Student Rentals", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01244 952252", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="Hello@bedstudentrentals.com", input_type="VALUE")
            
        
        script_data = response.xpath("//script[contains(.,'numberOfRooms')]/text()").get()
        if script_data:
            data = json.loads(script_data)[0]
            city = data["address"]["addressRegion"]
            item_loader.add_value("city", city)

        ext_id = response.url.split("-")[-1].split("/")[0].strip()
        if ext_id:
            item_loader.add_value("external_id", ext_id)
        
        yield item_loader.load_item()
