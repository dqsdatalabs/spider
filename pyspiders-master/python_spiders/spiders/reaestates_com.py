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
    name = 'reaestates_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.reaestates.com/search?sta=toLet&st=rent&pt=residential&stygrp=3&stygrp=6",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.reaestates.com/search?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=8&stygrp=9",
                ],
                "property_type" : "house"
            },
            # {
            #     "url" : [
            #         "https://www.reaestates.com/search?sta=toLet&st=rent&pt=residential&stygrp=7",
            #     ],
            #     "property_type" : "room"
            # },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='PropBox-spacing']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[contains(@class,'paging-next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        property_type = response.meta.get('property_type')
        prop_type = response.xpath("//th[contains(.,'Style')]/following-sibling::td/text()[contains(.,'House')]").get()
        if prop_type:
            property_type = "house"
        item_loader.add_value("property_type", property_type)
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title) 

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Reaestates_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//th[contains(.,'Address')]/following-sibling::td/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//span[@class='locality']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//span[@class='postcode']/text()", input_type="F_XPATH", replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//th[contains(.,'Deposit')]/following-sibling::td/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        # ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//th[contains(.,'Rent')]/following-sibling::td/text()", input_type="M_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//th[contains(.,'Bedroom')]/following-sibling::td/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//th[contains(.,'Bathroom')]/following-sibling::td/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//th[contains(.,'Furnished')]/following-sibling::td/text()[not(contains(.,'Un'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='slideshow-thumbs']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@class='property-features']//li[contains(.,'Terrace') or contains(.,'terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@class='property-features']//li[contains(.,'Balcon') or contains(.,'balcon')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[@class='property-features']//li[contains(.,'Lift') or contains(.,'lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='property-features']//li[contains(.,'Parking') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
 
        energy_label = response.xpath("//th[contains(.,'EPC')]/following-sibling::td//a/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("/")[0])
        available_date = response.xpath("//th[contains(.,'Available')]/following-sibling::td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%B/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        desc = " ".join(response.xpath("//div[@class='property-description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//meta[@property='og:latitude']/@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//meta[@property='og:longitude']/@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="REA ESTATES", input_type="VALUE")

        city = response.xpath("//h1//span[@class='locality']/text()").get()
        if city and "belfast" in city.lower():
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="442890232000", input_type="VALUE")
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="hello@reaestates.com", input_type="VALUE")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="442891454578", input_type="VALUE")
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="gillian@reaestates.com", input_type="VALUE")
        
        yield item_loader.load_item()