# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'coldwellbanker_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.coldwellbanker.ie/wp-admin/admin-ajax.php?action=wppd_property_fetch&payload={%22page%22:1,%22price_min%22:%220%22,%22price_max%22:%2212000000%22,%22property_market%22:%22residential%22,%22property_newdev%22:%22no%22,%22type%22:[%22Apartment%22],%22status%22:%22To%20Let%22,%22location%22:[],%22baths%22:%220%22,%22order%22:%22date|DESC%22}",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.coldwellbanker.ie/wp-admin/admin-ajax.php?action=wppd_property_fetch&payload={%22page%22:1,%22price_min%22:%220%22,%22price_max%22:%2212000000%22,%22property_market%22:%22residential%22,%22property_newdev%22:%22no%22,%22type%22:[%22House%22],%22status%22:%22To%20Let%22,%22location%22:[],%22baths%22:%220%22,%22order%22:%22date|DESC%22}",
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

        for item in response.xpath("//div[@class='property-card--title']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Coldwellbanker_PySpider_ireland", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"property/":1, "/":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h2/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h2/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h2/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//em[contains(.,'€')]/text()", input_type="F_XPATH", get_num=True, split_list={"€":1, " ":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'BEDROOM')]/following-sibling::em/text()", input_type="M_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'BATH')]/following-sibling::em/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        
        desc = " ".join(response.xpath("//div[@id='property-description']//text()").getall())
        if desc:
            desc = re.sub(r'\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        address = response.xpath("//script[contains(., 'address')]/text()").re_first(r'streetAddress": "(.*)"')
        if address:
            item_loader.add_value("address", address)
        city = response.xpath("//script[contains(., 'address')]/text()").re_first(r'"addressLocality": "(.*)"')
        if city:
            item_loader.add_value("city", city)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'latitude')]//text()", input_type="F_XPATH", split_list={'latitude":':1, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'latitude')]//text()", input_type="F_XPATH", split_list={'longitude":':1, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel-cell']//@src", input_type="M_XPATH")
        
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Coldwell Banker Carlton Estates", input_type="VALUE")
        if response.xpath("//p//text()[contains(.,'-')]").get():
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//p//text()[contains(.,'-')]", input_type="F_XPATH")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="353 1 4110012", input_type="VALUE")
        mail = response.xpath("//a[contains(@href, 'mail')]/@href").re_first(r"mailto:(.*)")
        if mail:
            item_loader.add_value("landlord_email", mail)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@coldwellbanker.ie", input_type="VALUE")
        
        yield item_loader.load_item()