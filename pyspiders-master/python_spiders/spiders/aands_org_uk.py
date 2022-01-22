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
    name = 'aands_org_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.aands.org.uk/search/{}.html?showsold=off&showstc=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Terraced",
                ],
                "property_type" : "apartment",
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
        for item in response.xpath("//a[@class='thumbnail']"):
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

        property_type  = "".join(response.xpath("//span[@itemprop='description']/p/text()[contains(.,'house')]").extract())
        if property_type:
            item_loader.add_value("property_type","house")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Aands_Org_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='property-long-description']/h3/following-sibling::*//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//h1/small/text()[contains(.,'Bedroom')]", input_type="F_XPATH", get_num=True, split_list={"Bedroom":0, "|":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//h1/small/text()[contains(.,'Bathroom')]", input_type="F_XPATH", get_num=True, split_list={"Bathroom":0, "|":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@itemprop='price']/strong/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='property-thumbnails']/div[@class='carousel-inner']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'ShowMap')]/text()", input_type="F_XPATH", split_list={"&q=":1, "%2C":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'ShowMap')]/text()", input_type="F_XPATH", split_list={"&q=":1, "%2C":1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="A & S Milward", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01773 825 788", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="enquiries@aands.org.uk", input_type="VALUE")

        external_id  = response.url.split("property-details/")[1].split("/")[0]
        if external_id:
            item_loader.add_value("external_id",external_id.strip())
        item_loader.add_xpath("city","substring-after(//li[3]//span[@itemprop='title']/text(),', ')")
       
        yield item_loader.load_item()