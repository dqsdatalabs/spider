# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear

class MySpider(Spider):
    name = 'regie_conseil_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Regie_Conseil_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.regie-conseil.fr/nos-annonces/?type_recherche=LO&natures=appartement&prix_min=0&prix_max=1891&surface_min=0&surface_max=521&saisonnier=0&neuf=0&numero_page=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.regie-conseil.fr/nos-annonces/?type_recherche=LO&natures=34630&prix_min=0&prix_max=1851&surface_min=0&surface_max=521&saisonnier=0&neuf=0&numero_page=1",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@id='annonce-block']//figure/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if page == 2 or seen:
            follow_url = response.url.replace("&numero_page=" + str(page - 1), "&numero_page=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("city", address.split("location à")[-1].strip())
            item_loader.add_value("address", address.split("location à")[-1].strip())
        room_count = response.xpath("//div[div[.='Chambre(s)']]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())    
        else:
            room_count = response.xpath("//div[div[i[@class='fas fa-door-open']]]/div[2]/span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())    
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.external_source, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//h2/text()", input_type="F_XPATH",split_list={"Référence":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[div[contains(.,'Etage')]]/div[2]/text()", input_type="F_XPATH", split_list={"ETAGE":0})
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'commentaireBien')]/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[div[i[@class='fas fa-arrows-alt']]]/div[2]/span/text()", input_type="F_XPATH", get_num=True,split_list={'.':0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[div[contains(.,'Salle d')]]/div[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[div[i[@class='fas fa-euro-sign']]]/div[2]/span[1]/text()", input_type="F_XPATH", get_num=True, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[div[contains(.,'Disponibilité')]]/div[2]/text()", input_type="F_XPATH", replace_list={"Immediate":"now"})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[span[.='Dépôt de garantie']]/span[2]/span[1]/text()", input_type="F_XPATH", get_num=True,split_list={'.':0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel-inner']//a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lat":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lng":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[span[.='Charges mensuelles']]/span[2]/span[1]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[div[contains(.,'Meublé')]]/div[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[div[contains(.,'Ascenseur')]]/div[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Regie Conseil", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 74 85 14 12", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="vienne@regie-conseil.fr", input_type="VALUE")
        energy_label = response.xpath("//div[img[contains(@src,'dpe_energie')]]//div[@class='class_curseur']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))  
        yield item_loader.load_item()
def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label