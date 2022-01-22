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
    name = 'immobiliarealterego_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Immobiliarealterego_PySpider_italy"
    start_urls = ['http://www.immobiliarealterego.it/affitto-casa-brescia']  # LEVEL 1
    
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='dettagli_immobile']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        prop_type = response.xpath("//div[@class='box_appartamento']/div/text()[contains(.,'Tipologia:')]").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        city = response.xpath("//a[@class='localita_appartamento']//text()").get()
        if city:
            item_loader.add_value("city", city)

        description = response.xpath("//p[contains(@class,'info_appartamento')]//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath("//span[contains(.,'Prezzo')]//text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("euro"))
        item_loader.add_value("currency", "EUR")

        bathroom_count = response.xpath("//b[contains(.,'Caratteristiche immobile')]//following-sibling::text()[contains(.,'Bagni')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bagni: ")[1])

        room_count = response.xpath("//b[contains(.,'Caratteristiche immobile')]//following-sibling::text()[contains(.,'Locali')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Locali: ")[1])

        square_meters = response.xpath("//b[contains(.,'Caratteristiche immobile')]//following-sibling::text()[contains(.,'Metri quadri:')]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("Metri quadri: ")[1])

        balcony = response.xpath("//b[contains(.,'Caratteristiche interne')]//following-sibling::text()[contains(.,'Balcone')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)

        terrace = response.xpath("//b[contains(.,'Caratteristiche interne')]//following-sibling::text()[contains(.,'Terrazzo')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            item_loader.add_value("terrace", False)

        furnished = response.xpath("//div[@id='breadcrumb']//span[@class='active_breadcrumb']//text()").get()
        if "arredato" in furnished.lower():
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        images = [response.urljoin(x) for x in response.xpath("//div[contains(@id,'miniature')]//a[contains(@class,'fancybox')]//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Alterego Immobiliare")
        item_loader.add_value("landlord_phone", "+39 030 8901500")
        item_loader.add_value("landlord_email", "info@immobiliarealterego.it")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("bilocale" in p_type_string.lower() or "trilocale" in p_type_string.lower() or "quadrilocale" in p_type_string.lower()):
        return "house"
    else:
        return None