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
    name = 'direct_housing_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source = "Direct_Housing_Co_PySpider_united_kingdom"
    custom_settings = {"HTTPCACHE_ENABLED": False}
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://direct-housing.co.uk/property-search/?address_keyword=&radius=1&department=residential-lettings&property_type=&bedrooms=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&lat=&lng=",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        seen = False
        for item in response.xpath("//div[@class='actions']/a/@href").getall():
            follow_url = item
   
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        # next_page = response.xpath("//a[@class='next page-numbers']/@href").get()
        # if next_page:
        #     p_url = next_page
        #     yield Request(
        #         p_url,
        #         callback=self.parse,
        #         meta={"property_type":response.meta["property_type"]})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Direct_Housing_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='elementor-widget-container']/h2/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='elementor-widget-container']/h2/text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='summary-contents']/text()", input_type="M_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value=response.meta["room_count"], input_type="VALUE", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='elementor-widget-bathrooms']/text()", input_type="F_XPATH", get_num=True, split_list={"bathroom":0, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/text()", input_type="M_XPATH", get_num=True, split_list={"£":1, " ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='date-available']/text()", input_type="F_XPATH", split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[contains(text(),'Deposit')]/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='thumbnailCarousel']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//li[contains(.,'EPC Rating')]/text()", input_type="F_XPATH", split_list={"Rating":1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,'furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Direct Housing", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0121 472 3331", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="lettings@direct-housing.co.uk", input_type="VALUE")


        # position = response.xpath("//a[contains(@title,'Open this')]/@href").get()
        # if position:
        #     lat = re.search("\?ll=([\d.]+),([-\d.]+)", position).group(1)
        #     long = re.search("\?ll=([\d.]+),([-\d.]+)", position).group(2)

        #     item_loader.add_value("latitude",lat)
        #     item_loader.add_value("longitude",long)



        if not item_loader.get_collected_values("deposit"):
            ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//text()[contains(.,'Deposit:')]", input_type="F_XPATH", get_num=True, split_list={"Deposit:":1, " ":0})
        
        desc = "".join(response.xpath("//div[@id='product-description']//text()").getall())
        room = response.xpath("//div[@class='elementor-widget-bedrooms']/text()").get()
        if room:
             item_loader.add_value("room_count", room)
        if not item_loader.get_collected_values("room_count"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//text()[contains(.,'bedroom')]", input_type="F_XPATH", get_num=True, split_list={"bedroom":0, " ":-1})
              
        if not item_loader.get_collected_values("parking"):
            ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//text()[contains(.,'street parking available')]", input_type="F_XPATH", tf_item=True)
        
        if not item_loader.get_collected_values("furnished"):
            ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//text()[contains(.,'fully furnished')]", input_type="F_XPATH", tf_item=True)

        yield item_loader.load_item()