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
    name = 'victoriaimmo_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    start_urls = ["http://www.victoriaimmo.fr/catalog/advanced_search_result.php?action=update_search&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27=&C_27_type=UNIQUE&C_27_temp=&C_27_temp_location=&C_33_search=SUPERIEUR&C_33_type=NUMBER&C_38_search=SUPERIEUR&C_38_type=NUMBER&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_123_search=CONTIENT&C_123_type=TEXT&C_123=&search_id=1716843997749169&&search_id=1716843997749169&page=1"]
    external_source='Victoriaimmo_PySpider_france'

    # 1. FOLLOWING
    def parse(self, response):
        for item in  response.xpath("//ul[@class='thumb']//li//a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
    
        next_page =response.xpath("//span[.='Suivante']/parent::u/parent::a/@href").get()
        print(next_page)
        if next_page:
            yield Request(
            response.urljoin(next_page),
            callback=self.parse,
           )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//div[@id='bien_immobilier']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//td[.='Type de bien']/following-sibling::td/text()").get()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))

        rent=response.xpath("//td[contains(.,'Loyer charges comprises')]/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("EUR")[0].strip())
        item_loader.add_value("currency","GBP")
        city=response.xpath("//td[contains(.,'Ville')]/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("city",city)       
        zipcode=response.xpath("//td[contains(.,'Code postal')]/following-sibling::td/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        if city and zipcode:
            item_loader.add_value("address",city+" "+zipcode)
        room_count=response.xpath("//td[contains(.,'Nombre pièces')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)       
        bathroom_count=response.xpath("//td[contains(.,'Salle(s) de bains')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        furnished=response.xpath("//td[contains(.,'Meublé')]/following-sibling::td/text()").get()
        if furnished and "Oui"==furnished:
            item_loader.add_value("furnished",True)
        square_meters=response.xpath("//td[contains(.,'Surface')]/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].split(".")[0])
        deposit=response.xpath("//span[@class='alur_location_depot']/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0])
        utilities=response.xpath("//span[@class='alur_location_honos']/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0])
        energy_label=response.xpath("//td[contains(.,'Consommation énergie primaire')]/following-sibling::td/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        external_id=response.xpath("//span[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1])
        description=response.xpath("//div[@class='right']/h3/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//img[@class='img_border']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Victoria Immobilier")
        item_loader.add_value("landlord_phone","04.43.55.04.99")
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