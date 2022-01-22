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
    name = 'aliga_cat'
    start_urls = ["https://www.aliga.cat/en/search-results/?status%5B%5D=for-rent"]
    execution_type='testing'
    country='spain'
    locale='es'
    external_source='Aliga_cat_PySpider_spain_es'
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'listing-view grid-view')]//div[@class='listing-thumb']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        prop_type = response.xpath("//li[@class='prop_type']/span/text()").get()
        if prop_type and "Piso" in prop_type:
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "dúplex" in prop_type.lower():
            item_loader.add_value("property_type", "house")
        else:
            return

        item_loader.add_xpath("title", "//div[@class='page-title']/h1/text()")

        external_id="".join(response.xpath("//div[strong[.='Referencia:']]/text()").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        item_loader.add_xpath("city", "//li[strong[.='Ciudad']]/span/text()")
        item_loader.add_xpath("zipcode", "//li[strong[.='Código postal ']]/span/text()")

        item_loader.add_xpath("external_id", "//li[strong[.='Referencia:']]/span/text()")
        item_loader.add_xpath("room_count", "//li[strong[.='Habitaciones:']]/span/text()|//li/strong[contains(.,'Room')]//following-sibling::span//text() | //li[strong[.='Bedroom:']]/span/text()")

        energy_label = response.xpath("//li[strong[.='Clasificación energética:']]/span/text()").get()
        if len(energy_label) == 1:
            item_loader.add_value("energy_label", energy_label)
        desc = "".join(response.xpath("//div[@class='block-content-wrap']/p/text()").extract())
        item_loader.add_value("description", desc.strip())

        bathroom_count = response.xpath("//i[contains(@class,'icon-bathroom')]/following-sibling::strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        images = [response.urljoin(x)for x in response.xpath("//div[@class='tab-content']//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        address="".join(response.xpath("//li[strong[.='Barrio']]/span/text() | //li[strong[.='Ciudad']]/span/text()").extract())
        if address:
            item_loader.add_value("address", address.strip())

        rent="".join(response.xpath("//li[strong[.='Precio:']]/span/text()").extract())
        if rent:
            item_loader.add_value("rent_string", rent.strip())

        meters="".join(response.xpath("//li[strong[.='Tamaño de la propiedad:']]/span/text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.strip().split("m2")[0])

        terrace=response.xpath("//li[strong[.='Garages:']]/span/text()").get()
        if terrace:
            item_loader.add_value("parking", True)

        terrace=response.xpath("//div[@class='block-content-wrap']/ul/li[contains(.,'Elevator')]").get()
        if terrace:
            item_loader.add_value("elevator", True)

        terrace=response.xpath("//div[@class='block-content-wrap']/ul/li[contains(.,'Balcony')]").get()
        if terrace:
            item_loader.add_value("balcony", True)

        terrace=response.xpath("//div[@class='block-content-wrap']/ul/li[contains(.,'Furniture')]").get()
        if terrace:
            item_loader.add_value("furnished", True)
        
        item_loader.add_value("landlord_phone", "00 34 934 518 831")
        item_loader.add_value("landlord_email", "contacto@aliga.cat")
        item_loader.add_value("landlord_name", "Aliga_cat")
                
        yield item_loader.load_item()

