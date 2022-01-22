# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'immobiliaremariella_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Immobiliaremariella_PySpider_italy"
    start_urls = [
        'https://www.immobiliaremariella.it/stato-immobile/affitto/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get('page', 2)

        seen = False
        for item in response.xpath("//figure/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True

        if page == 2 or seen:
            url = f"https://www.immobiliaremariella.it/stato-immobile/affitto/page/{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//span/small/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value(
                "property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)


        external_id = response.xpath(
            "(//span[contains(@title,'ID Immobile')]//text())[10]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.replace("\n",""))

        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title.replace("\u00a0", ""))

        description = response.xpath(
            "//div[@class='content clearfix']//p//text()").getall()
        if description:
            item_loader.add_value("description", description)

        square_meters = response.xpath(
            "(//span[contains(@title,'Superficie')]//text())[5]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
            
        room_count = response.xpath(
            "(//span[contains(.,'Camere')]//text())[4]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Camere")[0])
        else:
            room_count = response.xpath(
            "(//span[contains(.,'Camera')]//text())[4]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("Camera")[0])

        bathroom_count = response.xpath(
            "//div[@class='property-meta clearfix']//span//text()[contains(.,'Bagno')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bagno")[0])

        address = response.xpath(
            "//address[contains(@class,'title')]//text()").get()
        if address:
            item_loader.add_value("address", address)

        city = response.xpath(
            "//address[contains(@class,'title')]//text()").get()
        if city and "," in city.lower():
            city=city.split(",")[-4:-3]
            item_loader.add_value("city", city)
        else:
            item_loader.add_value("city", "Genova")

        zipcode = response.xpath(
            "//address[contains(@class,'title')]//text()").get()
        if zipcode and "," in zipcode.lower():
            zipcode=zipcode.split(",")[-2:-1]
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath(
            "//span[@class='price-and-type']//text()[contains(.,'€')]").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[1].split(",")[0])
        item_loader.add_value("currency", "EUR")

        images = [response.urljoin(x) for x in response.xpath(
            "//ul[@class='slides']//li//a//@href").getall()]
        if images:
            item_loader.add_value("images", images)
            
        balcony = response.xpath(
            "//h4[contains(.,'Caratteristiche')]//following-sibling::ul//li//text()[contains(.,'Balcone')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)

        elevator = response.xpath(
            "//li[contains(@id,'rh_property')]//a[contains(.,'Ascensore')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        else:
            item_loader.add_value("elevator", False)
            
        parking = response.xpath(
            "//h4[contains(.,'Caratteristiche')]//following-sibling::ul//li//text()[contains(.,'Cantina')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)
            
        furnished = response.xpath(
            "//h4[contains(.,'Caratteristiche')]//following-sibling::ul//li//text()[contains(.,'Arredato')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False) 

        item_loader.add_value("landlord_name", "Immobiliare Mariella")
        item_loader.add_value("landlord_phone", "010/7404554")
        item_loader.add_value("landlord_email", "immobiliaremariella@gmail.com")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "casa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    else:
        return None
