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
    name = 'accommodationforstudents_com'
    execution_type='testing'
    country='australia'
    locale='en'
    custom_settings = {
        "PROXY_ON": "True"
    }
    def start_requests(self):
        start_urls = [
            {
                "url": ["https://www.accommodationforstudents.com/search-results?location=any&beds=0&searchType=flat&price=&lettingPeriod=academicYear&geo=false&page=1"],
                "property_type": "apartment"
            },
	        {
                "url": ["https://www.accommodationforstudents.com/search-results?location=any&beds=0&searchType=house&price=&lettingPeriod=academicYear&geo=false&page=1"],
                "property_type": "house"
            },
            {
                "url": ["https://www.accommodationforstudents.com/search-results?location=London&area=&beds=1&searchType=studio&price=undefined&limit=12&lettingPeriod=academicYear"],
                "property_type": "studio"
            },
            {
                "url": ["https://www.accommodationforstudents.com/search-results?location=London&area=&beds=1&searchType=halls&price=undefined&limit=12&lettingPeriod=academicYear"],
                "property_type": "room"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//article/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            x = None
            if response.meta.get('property_type')=="apartment": x="flat"
            if response.meta.get('property_type')=="house": x="house"
            if response.meta.get('property_type')=="studio": x="studio"
            if x:
                url = f"https://www.accommodationforstudents.com/search-results?location=any&beds=0&searchType={x}&price=&lettingPeriod=academicYear&geo=false&page={page}"
                yield Request(url, callback=self.parse, meta={"page": page+12, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Accommodationforstudents_PySpider_united_kingdom")

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[@class='address-with-link__line']/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[@class='address-with-link__line'][last()]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[@class='address-with-link__line'][3]/text()", input_type="F_XPATH", replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'property-details__overview')]//p//text()", input_type="M_XPATH")
        
        if response.meta.get('property_type') == "room" or response.meta.get('property_type') == "studio":
            item_loader.add_value("room_count", "1")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[@class='rooms-available__count']/text()", input_type="F_XPATH", get_num=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//h1/text()[contains(.,'bath')]", input_type="F_XPATH", get_num=True, split_list={"bath":0, " ":-1})
        
        deposit = response.xpath("//span[@class='price-info__amount price-info__amount--deposit']/text()").get()
        if deposit:
            deposit = deposit.replace("£","").strip()
            item_loader.add_value("deposit",deposit)
        term = response.xpath("//span[@class='price-info__link']/text()").get()
        if term:
            term = term.replace("/","")
            if "pw" in term.lower():
                ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='price-info__amount price-info__amount--rent']/text()", input_type="F_XPATH", per_week=True, get_num=True, split_list={".":0}, replace_list={"£":""})
            else:
                ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='price-info__amount price-info__amount--rent']/text()", input_type="F_XPATH", get_num=True, split_list={".":0}, replace_list={"£":""})
                

        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='property-header__availability']/span/text()", input_type="F_XPATH", replace_list={"Available":"", "from":""})
        if "property/" in response.url:
            ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"property/":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'image-gallery-thumbnail')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'latitude')]/text()", input_type="F_XPATH", split_list={'latitude": "':1,'"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'latitude')]/text()", input_type="F_XPATH", split_list={'longitude": "':1,'"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'garage') or contains(.,'Garage')]//@fill[contains(.,'#6cc2b7')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'balcon') or contains(.,'Balcon')]//@fill[contains(.,'#6cc2b7')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]//@fill[contains(.,'#6cc2b7')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Lift')]//@fill[contains(.,'#6cc2b7')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'terrace') or contains(.,'Terrace')]//@fill[contains(.,'#6cc2b7')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li[contains(.,'Pool') or contains(.,'pool')]//@fill[contains(.,'#6cc2b7')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//li[contains(.,'Washing')]//@fill[contains(.,'#6cc2b7')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//li[contains(.,'Dishwasher')]//@fill[contains(.,'#6cc2b7')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Accommodation for Students", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="07957 785 740", input_type="VALUE")
        
        yield item_loader.load_item()