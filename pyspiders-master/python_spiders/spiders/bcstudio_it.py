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
    name = 'bcstudio_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Bcstudio_PySpider_italy"
    start_urls = ['https://bcstudio.it/real-estate/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'Dettagli')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//tr[th[contains(.,'TIPOLOGIA')]]/td/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath(
            "//title//text()").get()
        if external_id:
            external_id=external_id.split(" ")[1:2]
            item_loader.add_value("external_id", external_id)
  
        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        city = response.xpath(
            "//table[@class='table m_20_t']//tr//th[contains(.,'PROVINCIA')]//following-sibling::td//text()").get()
        if city:
            item_loader.add_value("city", city)

        address = response.xpath(
            "//table[@class='table m_20_t']//tr//th[contains(.,'UBICAZIONE')]//following-sibling::td//text()").get()
        if address:
            item_loader.add_value("address", address)
        else:
            if city:
                item_loader.add_value("address", city)

        description = response.xpath(
            "//div[@class='entry-content']//p//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath(
            "//table[@class='table m_20_t']//tr//th[contains(.,'PREZZO')]//following-sibling::td//text()").get()
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath(
            "//table[@class='table m_20_t']//tr//th[contains(.,'MQ')]//following-sibling::td//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        bathroom_count = response.xpath(
            "//table[@class='table m_20_t']//tr//th[contains(.,'BAGNI')]//following-sibling::td//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        room_count = response.xpath(
            "//table[@class='table m_20_t']//tr//th[contains(.,'VANI')]//following-sibling::td//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        images = [response.urljoin(x) for x in response.xpath(
            "//div[contains(@class,'col-lg-6 col-md-4 col-xs-12')]//img[contains(@class,'w_10_p h_a')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "BC Studio")
        item_loader.add_value("landlord_phone", " 055 010 78 43")
        item_loader.add_value(
            "landlord_email", "bcstudiorealestatesas@legalmail.it")

        
        status = response.xpath("//tr[th[contains(.,'CONTRATTO')]]/td/text()").get()
        if "affitto" in status.lower():
            yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("casa" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower() or "mansarda" in p_type_string.lower()):
        return "house"
    else:
        return None