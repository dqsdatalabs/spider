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
    name = 'immobiliarebelvedere_eu'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Immobiliarebelvedere_PySpider_italy"
    start_urls = ["https://www.immobiliarebelvedere.eu/ricerca-immobile/?status=affitto"]


    # 1. FOLLOWING
    def parse(self, response):        
        for item in response.xpath("//div[contains(@class,'card__wrap')]"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Vedi immobile')]/@href").get())
            property_type = item.xpath(".//h3/a/text()").get()
            if get_p_type_string(property_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        external_id=response.xpath("//p[contains(.,'ID Immobile')]/following-sibling::p/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].replace("\u00a0",""))

        title="".join(response.xpath("//div[@class='rh_content']/p/text()").getall())
        if title:
            item_loader.add_value("title",title.split(":")[0])
        else:
            title="".join(response.xpath("(//meta[@property='og:description']/@content)[1]").getall())
            if title:
                item_loader.add_value("title",title)

        rent=response.xpath("//p[@class='price']/text()").get()
        if rent:
            rent=rent.split("€")[0].replace(",","")
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency", "EUR")

        room_count=response.xpath("//div[@class='rh_property__meta']/h4[contains(.,'Vani')]/parent::div/div/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        item_loader.add_value("city", "Florence")
        address="".join(response.xpath("//div[@class='rh_content']/p/text()").getall())
        if address:
            item_loader.add_value("address",address.split(":")[0])
        
        
        desc=response.xpath("//div[@class='rh_content']/p/text()").get()
        if desc:
            item_loader.add_value("description",desc)
        images=response.xpath("//li//a[@data-gall='gallery_real_homes']/img/@src").getall()
        if images:
            item_loader.add_value("images",images)
        square_meters=response.xpath("//h4[contains(.,'Superficie')]/following-sibling::div/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        energy_label=response.xpath("//p[contains(.,'Classe energetica')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split(":")[-1])
        bathroom_count=response.xpath("//h4[contains(.,'Vani')]/following-sibling::div/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        
        item_loader.add_value("landlord_name","Immobiliare Belvedere")
        item_loader.add_value("landlord_phone","055 088 0658 - 055 0946039")
        item_loader.add_value("landlord_email","info@immobiliarebelvedere.eu")
        
        yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartamento" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("casa" in p_type_string.lower() or "villetta" in p_type_string.lower() or "semindipendente" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None