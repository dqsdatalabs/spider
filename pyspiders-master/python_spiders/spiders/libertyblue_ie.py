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
    name = 'libertyblue_ie'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "Libertyblue_PySpider_ireland"

    def start_requests(self):
        url = "https://libertyblue.ie/wp-admin/admin-ajax.php?action=wppd_property_fetch&payload={%22page%22:1,%22per_page%22:%2248%22,%22price_min%22:%220%22,%22price_max%22:%22900000%22,%22type%22:[%22Apartment%22,%22House%22],%22location%22:[],%22status%22:%22To%20Let%22,%22baths%22:%220%22,%22order%22:%22date|DESC%22}"
        yield Request(url, callback=self.parse)   

    # 1. FOLLOWING
    def parse(self, response):
        for url in response.xpath("//div[@class='property-card--title']/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        property_type=response.xpath("//span[.='Type']/following-sibling::em/text()").get()
        if property_type and "house" in property_type.lower():
            item_loader.add_value("property_type","house")
        rent=response.xpath("//span[.='Price']/following-sibling::em/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1].replace(",",""))
        item_loader.add_value("currency","GBP")
        room_count=response.xpath("//span[contains(.,'BEDROOM')]/following-sibling::em/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[contains(.,'BATHROOM')]/following-sibling::em/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        adres=response.xpath("//h1[@class='single-property-title single-property-title--default']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        images=[x for x in response.xpath("//div//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description=response.xpath("//div[@id='property-description']/p/text()").get()
        if description:
            item_loader.add_value("description",description)

        name=response.xpath("//h4[@class='strip-sidebar-agent-name has-agent-name']/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//a[@class='strip-link']/@href").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[1])
        email=response.xpath("//a[@class='strip-sidebar-agent--email']/@href").get()
        if email:
            item_loader.add_value("landlord_email",email)
        yield item_loader.load_item()