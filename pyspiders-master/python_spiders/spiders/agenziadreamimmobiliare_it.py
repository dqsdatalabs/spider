# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from types import resolve_bases
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re 

class MySpider(Spider):
    name = 'agenziadreamimmobiliare_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Agenziadreamimmobiliare_PySpider_italy"
    start_urls = ['https://www.agenziadreamimmobiliare.it/affitto']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//form[@action='/affitto']//table"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@title='Successivo']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//p[contains(.,'Tipologia')]/strong/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("//h2/text()").get()
        item_loader.add_value("title", title)


        external_id = response.xpath("//p/text()[contains(.,'Rif.')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())

        price = response.xpath("//tr[td[contains(.,'Canone')]]/td[last()]/strong/text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price)


        square = response.xpath("//p/text()[contains(.,'Superficie')]/following-sibling::strong[1]/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m")[0])

        images = [x for x in response.xpath("//div[@id='jea-gallery-scroll']//a/@href").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_xpath("room_count","//p/text()[contains(.,'Vani')]/following-sibling::strong[1]/text()")
        item_loader.add_xpath("floor","//p/text()[contains(.,'Piani')]/following-sibling::strong[1]/text()")
        item_loader.add_xpath("bathroom_count","//p/text()[contains(.,'Bagni')]/following-sibling::strong[1]/text()")

    
        desc = " ".join(response.xpath("//div[@class='item_description']//p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        
        elevator = "".join(response.xpath("//li[contains(.,'Ascensore')]/text()").extract())
        if elevator:
            item_loader.add_value("elevator", True)
        parking = "".join(response.xpath("//li[contains(.,'Posto auto')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True)
            
        city =" ".join(response.xpath("normalize-space(//div/h3[.='Indirizzo']/following-sibling::strong[1]//text()[2])").extract())
        if city:
            item_loader.add_value("city", city.split(",")[-1].strip()) 
        address=" ".join(response.xpath("//div/h3[.='Indirizzo']/following-sibling::strong[1]//text()").getall())
        if address:
            address=address.strip().replace("\n","")
            address=re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address",address) 
 
        item_loader.add_value("landlord_phone", "0817111780")
        item_loader.add_value("landlord_email", "agenziadreamimmobiliare@gmail.com")
        item_loader.add_value("landlord_name", "Dream Immobiliare")

        if "arredato" in title:
            item_loader.add_value("furnished",True)


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("trilocale" in p_type_string.lower() or "house" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villino" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    else:
        return None