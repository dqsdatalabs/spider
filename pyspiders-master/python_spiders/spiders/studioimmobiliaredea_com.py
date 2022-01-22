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
    name = 'studioimmobiliaredea_com'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Studioimmobiliaredea_PySpider_italy"
    start_urls = ['https://www.studioimmobiliaredea.com/locazioni/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'j-module n j-hgrid')]"):
            url = item.xpath(".//a[contains(@class,'calltoaction-link')]/@href[.!='/']").get()
            if url:
                follow_url = response.urljoin(url)
                status = item.xpath(".//figure//img[contains(@src,':format=jpg')]//@src").get()
                if status:
                    yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.studioimmobiliaredea.com/locazioni-pagina{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = "".join(response.xpath("//strong[contains(.,'Tipologia')]/following-sibling::text() | //strong[contains(.,'Tipologia')]/following-sibling::em/text()").getall())
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        elif get_p_type_string(response.url):
            item_loader.add_value("property_type", get_p_type_string(response.url))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")
        external_id=response.xpath("//em[contains(.,'RIF')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(".")[-1])

        square_meters=response.xpath("//em[.='Mq:']/parent::strong/following-sibling::em/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        rent=response.xpath("//em[.='Richiesta:']/parent::strong/following-sibling::em/span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace("€",""))
        images=[x for x in response.xpath("//li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description=response.xpath("//p[contains(.,'CENTRO ')]/text() | //p[contains(.,'STRUPPA ')]/text()").get()
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//li[contains(text(),'Camer')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split()[0].strip())
        bathroom_count=response.xpath("//li[contains(.,'Bagno')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count","1")
        balcony=response.xpath("//li[contains(.,'balcone')]/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        furnishedcheck=item_loader.get_output_value("description")
        if furnishedcheck and "arredati" in furnishedcheck:
            item_loader.add_value("furnished",True)
        item_loader.add_value("landlord_name","DEA IMMOBILIARE")
        item_loader.add_value("landlord_phone","010 2425763")
        item_loader.add_value("landlord_email","mail@studioimmobiliaredea.com")
        item_loader.add_value("currency","EUR")

        address=response.xpath("//em[.='Località:']/parent::strong/following-sibling::em/text()").get()
        if address:
            item_loader.add_value("address",address)  
        item_loader.add_value("city","Roma")

        floor=response.xpath("//em[.='Piano:']/parent::strong/following-sibling::em/text()").get()
        if floor:
            floor = convert_floor_number(floor)
            item_loader.add_value("floor",str(floor))

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "loft" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    else:
        return None


def convert_floor_number(number_text):
    if number_text == "Primo":
        return 1
    elif number_text == "Secondo":
        return 2

    elif number_text == "Terzo":
        return 3
    
    elif number_text == "Quarto":
        return 4
    elif number_text == "Quinto":
        return 5
    elif number_text == "Sesto":
        return 6
    else: 
        return 7