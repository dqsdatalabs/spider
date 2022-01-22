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
    name = 'immobiliaremasterealestate_com'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Immobiliaremasterealestate_PySpider_italy"
    start_urls = ['http://www.immobiliaremasterealestate.com/proprieta/affitto/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//article//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = "".join(response.xpath("//h1/text() | //div[@class='wpb_wrapper']//p//text()").getall())
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        elif get_p_type_string(response.url):
            item_loader.add_value("property_type", get_p_type_string(response.url))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//div[@class='mkdf-st-inner']//h1[@class='mkdf-st-title']//text()[1]").get()
        if title:
            item_loader.add_value("title",title)

        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//div[@class='mkdf-icon-box-text-holder']//div[contains(.,'Metri quadri')]//following-sibling::div[1]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)

        room_count=response.xpath("//div[@class='mkdf-icon-box-text-holder']//div[contains(.,'Locali ')]//following-sibling::div[1]//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        bathroom_count=response.xpath("//div[@class='mkdf-icon-box-text-holder']//div[contains(.,'Bagni')]//following-sibling::div[1]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        description=response.xpath("//div[@class='wpb_wrapper']//p//text()").getall()
        if description:
            item_loader.add_value("description",description)

            images = [response.urljoin(x)for x in response.xpath("//img[contains(@class,'vc_single_image-img attachment-full')]//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_phone", "02 4800 8635")
        item_loader.add_value("landlord_name", "MASTER REAL ESTATE")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartamento" in p_type_string.lower() or "bilocale" in p_type_string.lower() or "trilocale" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "attico" in p_type_string.lower():
        return "apartment"
    elif p_type_string and ("casa indipendente" in p_type_string.lower() or "loft" in p_type_string.lower() or "quadrilocale" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    else:
        return None