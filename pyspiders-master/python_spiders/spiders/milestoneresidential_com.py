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
    name = 'milestoneresidential_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.milestoneresidential.com/search/{}.html?instruction_type=Letting&place=&latitude=&longitude=&bounds=&minprice=&maxprice=&property_type=Apartment",
                    "https://www.milestoneresidential.com/search/{}.html?instruction_type=Letting&place=&latitude=&longitude=&bounds=&minprice=&maxprice=&property_type=Flat",
                    "https://www.milestoneresidential.com/search/{}.html?instruction_type=Letting&place=&latitude=&longitude=&bounds=&minprice=&maxprice=&property_type=Flat+-+Conversion",
                    "https://www.milestoneresidential.com/search/{}.html?instruction_type=Letting&place=&latitude=&longitude=&bounds=&minprice=&maxprice=&property_type=Flat+-+Purpose+Built",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.milestoneresidential.com/search/{}.html?instruction_type=Letting&place=&latitude=&longitude=&bounds=&minprice=&maxprice=&property_type=House",
                    "https://www.milestoneresidential.com/search/{}.html?instruction_type=Letting&place=&latitude=&longitude=&bounds=&minprice=&maxprice=&property_type=House+-+Detached",
                    "https://www.milestoneresidential.com/search/{}.html?instruction_type=Letting&place=&latitude=&longitude=&bounds=&minprice=&maxprice=&property_type=House+-+End+Terrace",
                    "https://www.milestoneresidential.com/search/{}.html?instruction_type=Letting&place=&latitude=&longitude=&bounds=&minprice=&maxprice=&property_type=House+-+Semi-Detached",
                    "https://www.milestoneresidential.com/search/{}.html?instruction_type=Letting&place=&latitude=&longitude=&bounds=&minprice=&maxprice=&property_type=House+-+Terraced",
                    "https://www.milestoneresidential.com/search/{}.html?instruction_type=Letting&place=&latitude=&longitude=&bounds=&minprice=&maxprice=&property_type=Maisonette", 
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.milestoneresidential.com/search/{}.html?instruction_type=Letting&place=&latitude=&longitude=&bounds=&minprice=&maxprice=&property_type=Flat+-+Studio",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='card__property']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Milestoneresidential_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='overview']/div/div[1]/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@id='overview']//*[name()='path' and starts-with(@d,'M35')]/../../following-sibling::text()[1]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@id='overview']//*[name()='path' and starts-with(@d,'M7')]/../../following-sibling::text()[1]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h1/../h2/span/@content", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'Available')]/text()", input_type="F_XPATH", replace_list={"Available":"", "!":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//a[contains(@href,'deposit')]/@href", input_type="F_XPATH", get_num=True, split_list={"deposit=":1, "&":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'latitude')]/text()", input_type="F_XPATH", split_list={'"latitude": "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'latitude')]/text()", input_type="F_XPATH", split_list={'"longitude": "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking') or contains(.,'Bike')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Milestone Residential", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='property-contact-details']//a[contains(@href,'tel')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@id='floorplans']//@src[not(contains(.,'epc'))]", input_type="M_XPATH")
         
        images = [response.urljoin(x.split('url(')[-1].split(')')[0]) for x in response.xpath("//div[@class='section__property-slider']//div[contains(@class,'swiper-wrapper')]/div/@style").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        yield item_loader.load_item()