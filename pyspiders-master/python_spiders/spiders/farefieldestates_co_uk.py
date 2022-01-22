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

class MySpider(Spider):
    name = 'farefieldestates_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://farefieldestates.co.uk/apartments-houses/?let_type=29&order_by=&property-type=Apartments&bedrooms=&min_price=295&max_price=2700",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://farefieldestates.co.uk/apartments-houses/?let_type=29&order_by=&property-type=Houses&bedrooms=&min_price=295&max_price=2700",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://farefieldestates.co.uk/rooms/",
                ],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='col-sm-6']"):
            status = item.xpath(".//div[contains(@class,'feature-overlay-text')]/text()").get()
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./div/a/@href").get())
            room_count = item.xpath("./div/a//p[contains(.,'Bedroom')]").get().split('Bedroom')[0] if item.xpath("./div/a//p[contains(.,'Bedroom')]").get() else None
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"], "room_count":room_count})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Farefieldestates_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='title']/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@class='title']/h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value=response.meta["room_count"], input_type="VALUE", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/p/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='gallery-slider__images']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@id='floor-plans']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,'furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Farefield Estates", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='sidebar']//a[contains(@href,'tel')]/p/text()", input_type="F_XPATH", split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="admin@renting.uk.com", input_type="VALUE")

        if response.xpath("//div[@id='features']//li[contains(.,'Bathroom')]").get(): item_loader.add_value("bathroom_count", 1)

        available_date = response.xpath("//div[@class='gallery']/div[@class='feature-countdown']/@data-date").get()
        if available_date:
            date_parsed = dateparser.parse(available_date)
            if date_parsed and date_parsed.year >= 2021:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        yield item_loader.load_item()