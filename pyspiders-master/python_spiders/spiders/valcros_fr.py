# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
from datetime import date
import dateparser

class MySpider(Spider):
    name = 'valcros_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    start_urls = ["https://www.valcros.fr/index_catalogue.php?geo_level=all&marche=1&transaction=2&surface_f=&terrain_f=&prix_f=&prix_t="]
    external_source='Valcros_PySpider_france'

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='lo-image-inner-cadre']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//h1//span//text()").getall()
        if title:
            item_loader.add_value("title",title)
        property_type="".join(response.xpath("//h1//span//text()").getall())
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        adres=response.xpath("//span[@class='line-2']/text()").get()
        if adres:
            item_loader.add_value("address",adres.split("-")[0].strip())
            item_loader.add_value("zipcode",adres.split("-")[1].split("/")[0])
        external_id=response.xpath("//span[@class='line-2']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1])
        description="".join(response.xpath("//div[@class='lo-box-content clearfix']/ul/following-sibling::p/text()").getall())
        if description:
            item_loader.add_value("description",description.replace("\t","").replace("\n",""))
        rent=response.xpath("//li[@class='li-price']/span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        item_loader.add_value("currency","GBP")
        images=[x for x in response.xpath("//div[@class='lo-image-inner-cadre']/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        square_meters=response.xpath("//span[contains(.,'Surface')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("de")[-1].split(" m2")[0].split(",")[0].strip())
        bathroom_count=response.xpath("//span[contains(.,'Salle de bains')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",square_meters.strip().split(" ")[0])
        deposit=response.xpath("//p[@class='details-descriptif-prix-honoraires']/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0].split(",")[0].strip())
        terrace=response.xpath("//span[.='Terrasse']").get()
        if terrace:
            item_loader.add_value("terrace",True)
        parking=response.xpath("//span[.='Parking']").get()
        if parking:
            item_loader.add_value("parking",True)
        
        yield item_loader.load_item()



def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maison" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None