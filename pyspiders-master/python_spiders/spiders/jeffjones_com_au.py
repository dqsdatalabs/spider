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
    name = 'jeffjones_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.mantisproperty.com.au/_agents/generic/realestate-rental-properties.aspx?searchbox=1&surr=False&beds=Any&bath=Any&cars=Any&price_from=0&price_to=0&type=Apartment&location=-&agent=219",
                    "https://www.mantisproperty.com.au/_agents/generic/realestate-rental-properties.aspx?searchbox=1&surr=False&beds=Any&bath=Any&cars=Any&price_from=0&price_to=0&type=Duplex&location=-&agent=219",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.mantisproperty.com.au/_agents/generic/realestate-rental-properties.aspx?searchbox=1&surr=False&beds=Any&bath=Any&cars=Any&price_from=0&price_to=0&type=House&location=-&agent=219",
                    "https://www.mantisproperty.com.au/_agents/generic/realestate-rental-properties.aspx?searchbox=1&surr=False&beds=Any&bath=Any&cars=Any&price_from=0&price_to=0&type=Townhouse&location=-&agent=219",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(.,'View the full details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Jeffjones_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(@id,'PropertyNum')]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//td[contains(.,'Location')]//following-sibling::td//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//td[contains(.,'Location')]//following-sibling::td//text()[2]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//span[contains(@id,'Title')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//span[contains(@id,'Desc')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//td//img[contains(@src,'bed')]//parent::td//following-sibling::td/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//td//img[contains(@src,'bath')]//parent::td//following-sibling::td/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//td[contains(.,'Rent')]//following-sibling::td//text()", input_type="F_XPATH",per_week=True, get_num=True, split_list={"$":1,".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//td[contains(.,'Availability')]//following-sibling::td//text()").get()
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                    
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@id,'gallery')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//td//img[contains(@src,'car')]//parent::td//following-sibling::td/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[contains(@class,'propfeatureslist')]//li[contains(.,'Balcony')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//div[contains(@class,'propfeatureslist')]//li[contains(.,'Dishwasher')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Jeff Jones Real Estate", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="07 3087 9750", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="help@jeffjones.com.au", input_type="VALUE")

        yield item_loader.load_item()